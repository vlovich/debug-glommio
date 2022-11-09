use std::{
    borrow::Borrow,
    cell::RefCell,
    ops::{Deref, Index},
    path::PathBuf,
    pin::Pin,
    rc::Rc,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    task::Waker,
    time::Duration,
};

use futures::{future::join_all, Future};

use glommio::{
    channels::{
        channel_mesh::{FullMesh, MeshBuilder, Receivers, Senders},
        shared_channel::ConnectedReceiver,
    },
    enclose,
    sync::Gate,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement, ResourceType, Shares, Task, TaskQueueHandle,
};
use more_asserts::assert_ge;
use nanorand::Rng;

macro_rules! trace {
    ($($tts:tt)*) => {
        #[cfg(feature = "debug_background_tasks")]
        println!($($tts)*);

        #[cfg(not(feature = "debug_background_tasks"))]
        if false {
            println!($($tts)*);
        }
    }
}

#[derive(Clone)]
pub enum MyError {
    Glommio(Rc<glommio::GlommioError<()>>),
}

impl From<glommio::GlommioError<()>> for MyError {
    fn from(err: glommio::GlommioError<()>) -> Self {
        MyError::Glommio(Rc::new(err))
    }
}

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyError::Glommio(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for MyError {}

impl std::fmt::Debug for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyError::Glommio(err) => write!(f, "{:#?}", err),
        }
    }
}

#[inline(always)]
pub(crate) fn erase_and_map_glommio_error<T>(e: glommio::GlommioError<T>) -> MyError {
    map_glommio_error(erase_glommio_error(e))
}

#[inline(always)]
pub(crate) fn map_glommio_error(e: glommio::GlommioError<()>) -> MyError {
    e.into()
}

pub(crate) fn erase_glommio_error<T>(e: glommio::GlommioError<T>) -> glommio::GlommioError<()> {
    match e {
        glommio::GlommioError::IoError(e) => glommio::GlommioError::IoError(e),
        glommio::GlommioError::EnhancedIoError {
            source,
            op,
            path,
            fd,
        } => glommio::GlommioError::EnhancedIoError {
            source,
            op,
            path,
            fd,
        },
        glommio::GlommioError::ExecutorError(kind) => glommio::GlommioError::ExecutorError(kind),
        glommio::GlommioError::BuilderError(kind) => glommio::GlommioError::BuilderError(kind),
        glommio::GlommioError::Closed(r) => glommio::GlommioError::Closed(erase_resource_type(r)),
        glommio::GlommioError::CanNotBeClosed(r, msg) => {
            glommio::GlommioError::CanNotBeClosed(erase_resource_type(r), msg)
        }
        glommio::GlommioError::WouldBlock(r) => {
            glommio::GlommioError::WouldBlock(erase_resource_type(r))
        }
        glommio::GlommioError::ReactorError(kind) => glommio::GlommioError::ReactorError(kind),
        glommio::GlommioError::TimedOut(elapsed) => glommio::GlommioError::TimedOut(elapsed),
    }
}

fn erase_resource_type<T>(r: ResourceType<T>) -> ResourceType<()> {
    match r {
        ResourceType::Semaphore {
            requested,
            available,
        } => ResourceType::Semaphore {
            requested,
            available,
        },
        ResourceType::RwLock => ResourceType::RwLock,
        ResourceType::Channel(m) => ResourceType::Channel(()),
        ResourceType::File(path) => ResourceType::File(path),
        ResourceType::Gate => ResourceType::Gate,
    }
}

/// A subset of DbError that's sent in response to an error processing background work.
#[derive(Clone, Debug)]
pub(crate) enum CrossThreadError {
    IOError(Arc<Mutex<glommio::GlommioError<()>>>),
}

impl From<CrossThreadError> for MyError {
    fn from(e: CrossThreadError) -> Self {
        match e {
            CrossThreadError::IOError(e) => MyError::Glommio(Rc::new(std::mem::replace(
                &mut e.lock().unwrap(),
                glommio::GlommioError::TimedOut(Duration::default()),
            ))),
        }
    }
}

impl From<MyError> for CrossThreadError {
    fn from(e: MyError) -> Self {
        match e {
            MyError::Glommio(mut e) => {
                CrossThreadError::IOError(Arc::new(Mutex::new(std::mem::replace(
                    Rc::get_mut(&mut e).expect("Error cloned can't be shared out"),
                    glommio::GlommioError::TimedOut(Duration::default()),
                ))))
            }
        }
    }
}

struct PendingRequestInner {}

/// Represents a request that has been sent to the background for processing. Awaiting on this
/// request will yield a `Result<InterThreadResponse, DbError>` indicating the successful response
/// for the background work or an error.
struct PendingRequest {
    response: RefCell<Option<Result<InterThreadResponse, MyError>>>,
    wakers: RefCell<Vec<Waker>>,
}

impl Default for PendingRequest {
    fn default() -> Self {
        Self {
            response: RefCell::default(),
            wakers: RefCell::default(),
        }
    }
}

impl PendingRequest {
    pub(crate) fn deliver(&self, r: Result<InterThreadResponse, MyError>) {
        {
            let mut response = self.response.borrow_mut();
            debug_assert!(response.is_none());
            *response = Some(r);
        }

        let mut borrow_wakers = self.wakers.borrow_mut();
        let wakers = std::mem::take(&mut *borrow_wakers);
        for waker in wakers {
            trace!(
                "{}: Notifying {:#?} of response",
                glommio::executor().id(),
                waker
            );
            waker.wake()
        }
    }
}

impl Future for &PendingRequest {
    type Output = Result<InterThreadResponse, MyError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(r) = self.response.borrow_mut().take() {
            return std::task::Poll::Ready(r);
        }

        let mut wakers = self.wakers.borrow_mut();
        if wakers.iter().find(|w| w.will_wake(cx.waker())).is_none() {
            wakers.push(cx.waker().clone());
        }

        std::task::Poll::Pending
    }
}

#[derive(Clone, Debug)]
pub(crate) enum BroadcastMessage {
    BackgroundThreadOnly,
    Closing,
}

#[derive(Clone, Debug)]
pub(crate) enum InterThreadRequest {
    #[cfg(test)]
    Ping { pong_delay: Option<Duration> },
}

#[derive(Clone, Debug)]
pub(crate) enum InterThreadResponse {
    #[cfg(test)]
    Pong,
}

/// Requests for work to a background thread or responses notifying of completion.
#[derive(Clone, Debug)]
pub(crate) enum InterThreadMessage {
    // Broadcast message to all threads. Broadcasts messages obviously don't get a response (one-way
    // async).
    Broadcast(BroadcastMessage),
    Request {
        id: usize,
        request: InterThreadRequest,
    },
    Response {
        id: usize,
        response: Result<InterThreadResponse, CrossThreadError>,
    },
}

struct WorkPoolConnectionState {
    background_only: bool,
    num_background_threads_total: usize,
    num_background_threads_checked_in: usize,
    waker: Option<Waker>,
    queue: TaskQueueHandle,
    receiver_tasks: Vec<Task<Result<(), MyError>>>,
    senders: Rc<Senders<InterThreadMessage>>,
    num_sent: usize,
    outstanding: Vec<(usize, Rc<PendingRequest>)>,
}

#[derive(Clone)]
pub(crate) struct WorkPoolConnectionImpl {
    inner: Rc<RefCell<WorkPoolConnectionState>>,
}

impl WorkPoolConnectionImpl {
    pub(crate) fn new(
        mesh_ipc: (
            Rc<Senders<InterThreadMessage>>,
            Receivers<InterThreadMessage>,
        ),
        num_background_threads_total: usize,
        background_only: bool,
    ) -> Self {
        let (senders, mut receivers) = mesh_ipc;

        let inner = Rc::new(RefCell::new(WorkPoolConnectionState {
            background_only,
            num_background_threads_total: if background_only {
                num_background_threads_total - 1
            } else {
                num_background_threads_total
            },
            num_background_threads_checked_in: 0,
            waker: None,
            queue: glommio::executor().create_task_queue(
                Shares::default(),
                glommio::Latency::NotImportant,
                "background",
            ),
            // We use a fully connected mesh so nr_consumers == nr_producers.
            receiver_tasks: Vec::with_capacity(senders.nr_consumers()),
            senders,
            outstanding: Vec::new(),
            num_sent: 0,
        }));

        let own_receiver_id = receivers.peer_id();
        let executor = glommio::executor();

        {
            let mut borrow = inner.as_ref().borrow_mut();

            for (peer, stream) in receivers.streams() {
                if peer == own_receiver_id {
                    continue;
                }

                let queue = borrow.queue;
                let async_state = inner.clone();
                let senders = borrow.senders.clone();

                borrow.receiver_tasks.push(
                    executor
                        .spawn_local_into(
                            async move {
                                Self::inter_thread_message_loop(
                                    &async_state,
                                    &senders,
                                    peer,
                                    stream,
                                )
                                .await
                            },
                            queue,
                        )
                        .unwrap_or_else(|_| panic!("Failed to create task for receiver {}", peer)),
                );
            }
        }

        Self { inner }
    }

    async fn handle(request: InterThreadRequest) -> Result<InterThreadResponse, MyError> {
        match request {
            #[cfg(test)]
            InterThreadRequest::Ping { pong_delay } => {
                if let Some(pong_delay) = pong_delay {
                    glommio::timer::Timer::new(pong_delay).await;
                }
                Ok(InterThreadResponse::Pong)
            }
        }
    }

    async fn inter_thread_message_loop(
        cur_shard_inner: &RefCell<WorkPoolConnectionState>,
        senders: &Senders<InterThreadMessage>,
        peer: usize,
        stream: ConnectedReceiver<InterThreadMessage>,
    ) -> Result<(), MyError> {
        let shutting_down = Gate::new();

        loop {
            let message = if let Some(message) = stream.recv().await {
                message
            } else {
                trace!("{}: Finished IPC stream", glommio::executor().id());
                break;
            };

            let _pass = shutting_down.enter().expect("Already signalled to close?");

            trace!(
                "{}: Peer {} -> {} Received {:#?}",
                glommio::executor().id(),
                peer,
                senders.peer_id(),
                message,
            );

            match message {
                InterThreadMessage::Broadcast(msg) => {
                    match msg {
                        BroadcastMessage::BackgroundThreadOnly => {
                            // TODO(soon): Shut down receiving messages from this task.
                            let mut borrow = cur_shard_inner.borrow_mut();
                            borrow.num_background_threads_checked_in += 1;
                            if borrow.num_background_threads_checked_in
                                == borrow.num_background_threads_total
                            {
                                if let Some(waker) = borrow.waker.take() {
                                    waker.wake();
                                }
                            }

                            if borrow.background_only {
                                // Background -> background messages don't seem like a good idea. My idea
                                // here is that foreground threads are responsible for distributing work. So
                                // if you want to migrate work between threads you'd have to thunk back out
                                // to the foreground thread (or more ideally you'd shard the work beforehand
                                // and send messages simultaneously to the different background threads and
                                // wait for the pieces you need to finish).
                                return Ok(());
                            }
                            // This thread is doing real work so this is just a check in we can just ignore
                            // I think. If for some reason we do need to know if the receiver is for a
                            // background thread, then we can memoize that here to adjust decisions later
                            // accordingly.
                            continue;
                        }
                        // This will be the last message that the foreground returns so we can stop reading.
                        BroadcastMessage::Closing => {
                            // Drop the gate.
                            std::mem::drop(_pass);

                            trace!(
                                "{}: Requested the gate close for this thread",
                                glommio::executor().id()
                            );
                            shutting_down.close().await.expect("Already closed?");
                            trace!(
                                "{}: Gate closed - terminating processing",
                                glommio::executor().id()
                            );
                            let mut borrow = cur_shard_inner.borrow_mut();
                            return Ok(());
                        }
                    }
                }
                InterThreadMessage::Request { id, request } => {
                    // Have to assign this to a named variable instead of just `_`.
                    // https://vojtechkral.github.io/blag/rust-drop-order/

                    let response = Self::handle(request).await;
                    trace!(
                        "{}: Sending response for request id {} to peer {}",
                        glommio::executor().id(),
                        id,
                        peer
                    );
                    senders
                        .send_to(
                            peer,
                            InterThreadMessage::Response {
                                id,
                                response: response.map_err(|e| e.into()),
                            },
                        )
                        .await
                        .map_err(erase_and_map_glommio_error)?;
                }
                InterThreadMessage::Response { id, response } => {
                    let mut borrow = cur_shard_inner.borrow_mut();
                    let idx = borrow
                        .outstanding
                        .binary_search_by_key(&id, |(id, _)| *id)
                        .expect("Response for message isn't outstanding!");
                    let pending = borrow.outstanding.remove(idx).1;
                    pending.deliver(response.map_err(|e| e.into()));
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn wait_until_none_outstanding(&mut self) -> Result<(), MyError> {
        loop {
            let (msg_id, outstanding) = {
                let borrow = self.inner.as_ref().borrow();
                trace!(
                    "{}: {} tasks still outstanding",
                    glommio::executor().id(),
                    borrow.outstanding.len()
                );
                assert!(
                    !borrow.background_only,
                    "Not valid to run from background thread"
                );
                if borrow.outstanding.is_empty() {
                    break;
                }

                (borrow.outstanding[0].0, borrow.outstanding[0].1.clone())
            };
            trace!(
                "{}: Waiting on response for message {} with waker {:#?}",
                glommio::executor().id(),
                msg_id,
                outstanding.wakers.borrow(),
            );
            let result = (&*outstanding).await;
            trace!(
                "{}: Got response {:#?} for message {}",
                glommio::executor().id(),
                result,
                msg_id
            );
            result?;
        }

        trace!(
            "{}: Finished waiting - no more outstanding messages",
            glommio::executor().id()
        );
        Ok(())
    }

    async fn close(self) -> Result<(), MyError> {
        trace!("{}: Closing current thread", glommio::executor().id());
        self.broadcast(BroadcastMessage::Closing)
            .await
            .unwrap_or_else(|e| {
                match &e {
                    // This thread already closed. Nothing to see here.
                    MyError::Glommio(e) => match e.deref() {
                        glommio::GlommioError::Closed(glommio::ResourceType::Channel(_)) => return,
                        _ => (),
                    },
                    _ => (),
                }
                panic!(
                    "{}: Failed to broadcast close: {:#?}",
                    glommio::executor().id(),
                    e
                )
            });

        let tasks = {
            let mut borrow = self.inner.as_ref().borrow_mut();
            trace!(
                "{}: Closing connection to senders",
                glommio::executor().id()
            );
            borrow.senders.close();
            assert_eq!(
                borrow.outstanding.len(),
                0,
                "Trying to close with outstanding tasks in flight"
            );
            std::mem::take(&mut borrow.receiver_tasks)
        };

        let pending = tasks.into_iter().map(|t| t);

        trace!(
            "{}: Waiting on {} pending receiver tasks",
            glommio::executor().id(),
            pending.len()
        );

        for r in join_all(pending).await {
            trace!(
                "{}: Pending task returned {:#?}",
                glommio::executor().id(),
                r
            );
            r?;
        }

        trace!(
            "{}: Finished waiting on all cancelled tasks",
            glommio::executor().id()
        );
        Ok(())
    }

    async fn wait_until_drained(self) -> Result<Self, MyError> {
        trace!(
            "{}: Waiting for all outstanding requests to complete",
            glommio::executor().id()
        );
        let tasks = { std::mem::take(&mut self.inner.as_ref().borrow_mut().receiver_tasks) };
        for r in join_all(tasks.into_iter()).await {
            trace!("{}: Task completed with {:#?}", glommio::executor().id(), r);
            r?;
        }

        Ok(self)
    }

    fn background_peer_to_send_to(num_consumers: usize, self_id: usize) -> usize {
        loop {
            let selected = nanorand::tls_rng().generate_range(0..num_consumers);
            if selected != self_id {
                return selected;
            }
        }
    }

    pub(crate) async fn broadcast(&self, msg: BroadcastMessage) -> Result<(), MyError> {
        let senders = {
            let borrow = self.inner.as_ref().borrow_mut();
            borrow.senders.clone()
        };

        let self_id = senders.peer_id();
        trace!(
            "{}: Broadcasting {:#?} to everyone but receiver {} ({} consumers)",
            glommio::executor().id(),
            msg,
            self_id,
            senders.nr_consumers()
        );
        let broadcasts = join_all(
            (0..senders.nr_consumers())
                .into_iter()
                .filter(|peer| *peer != self_id)
                .map(|peer| senders.send_to(peer, InterThreadMessage::Broadcast(msg.clone()))),
        )
        .await;

        for did_broadcast in broadcasts {
            did_broadcast.map_err(erase_and_map_glommio_error)?;
        }

        Ok(())
    }

    pub(crate) async fn distribute_work(
        &self,
        request: InterThreadRequest,
    ) -> Result<InterThreadResponse, MyError> {
        let pending_request = {
            let mut borrow = self.inner.as_ref().borrow_mut();
            let request_id = borrow.num_sent;
            borrow.num_sent += 1;
            borrow
                .outstanding
                .push((request_id, Rc::new(PendingRequest::default())));

            let pending_request = borrow.outstanding[request_id].1.clone();
            let peer = Self::background_peer_to_send_to(
                borrow.senders.nr_consumers(),
                borrow.senders.peer_id(),
            );

            let msg = InterThreadMessage::Request {
                id: request_id,
                request,
            };

            trace!(
                "{}: Peer {} -> {} Sending message {:#?}",
                glommio::executor().id(),
                borrow.senders.peer_id(),
                peer,
                msg,
            );

            borrow
                .senders
                .send_to(peer, msg)
                .await
                .map_err(erase_and_map_glommio_error)?;
            pending_request
        };

        pending_request.as_ref().await
    }
}

pub struct WorkPoolConnection {
    pub(crate) inner: WorkPoolConnectionImpl,
}

struct BackgroundCheckinComplete {
    connections: Rc<RefCell<WorkPoolConnectionState>>,
}

impl Future for BackgroundCheckinComplete {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut borrow = self.connections.borrow_mut();
        if borrow.num_background_threads_checked_in == borrow.num_background_threads_total {
            std::task::Poll::Ready(())
        } else {
            borrow.waker = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
}

#[derive(Clone)]
pub struct WorkPool {
    pub(crate) mesh: FullMesh<InterThreadMessage>,
    num_active_threads: usize,
    num_background_threads: usize,
}

impl WorkPool {
    pub fn new(
        num_active_threads: usize,
        num_background_threads: usize,
        work_queue_depth_per_peer: usize,
    ) -> Self {
        assert_ge!(
            num_active_threads,
            1,
            "Need at least one foreground thread designated to do the work"
        );
        assert_ge!(
            num_background_threads,
            1,
            "Need at least one background executor that's not the one for the database shard."
        );

        let num_total_threads = num_active_threads + num_background_threads;

        Self {
            mesh: MeshBuilder::full(num_total_threads, work_queue_depth_per_peer),
            num_active_threads,
            num_background_threads,
        }
    }

    async fn connect_work_pool(self, background_only: bool) -> Result<WorkPoolConnection, MyError> {
        let mesh_ipc = self.mesh.join().await.map_err(map_glommio_error)?;
        let mesh_ipc = (Rc::new(mesh_ipc.0), mesh_ipc.1);

        trace!(
            "{}: Sender id {}, receiver id {}",
            glommio::executor().id(),
            mesh_ipc.0.peer_id(),
            mesh_ipc.1.peer_id()
        );

        let connection =
            WorkPoolConnectionImpl::new(mesh_ipc, self.num_background_threads, background_only);

        if background_only {
            trace!(
                "{}: Advertising self as background thread",
                glommio::executor().id()
            );
            connection
                .broadcast(BroadcastMessage::BackgroundThreadOnly)
                .await?;
            trace!(
                "{}: Sent out advertisement for self as background thread",
                glommio::executor().id()
            );
        }

        BackgroundCheckinComplete {
            connections: connection.inner.clone(),
        }
        .await;

        trace!("{}: Mesh initialized", glommio::executor().id());

        Ok(WorkPoolConnection { inner: connection })
    }

    async fn join_work_pool_as_background(self) -> Result<(), MyError> {
        let connection = self.connect_work_pool(true).await?.inner;

        trace!(
            "{}: Background thread waiting for receiver tasks to terminate",
            glommio::executor().id()
        );
        let connection = connection.wait_until_drained().await?;
        trace!("{}: Background thread closing", glommio::executor().id());
        connection.close().await
    }

    pub fn start<F1, Fut1, F2, Ret>(
        self,
        foreground: F1,
        background: F2,
    ) -> Result<Vec<Ret>, MyError>
    where
        F1: FnOnce(WorkPoolConnection) -> Fut1 + Clone + Send + 'static,
        Fut1: Future<Output = Ret>,
        F2: FnOnce() -> Ret + Clone + Send + 'static,
        Ret: Send + 'static,
    {
        let capture = self;
        let active_threads_started = Arc::new(AtomicUsize::new(0));
        let results = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(capture.mesh.nr_peers(), CpuSet::online().ok()))
          .on_all_shards(enclose!((capture) move || async move {
            if active_threads_started.fetch_add(1, std::sync::atomic::Ordering::AcqRel) < capture.num_active_threads {
                trace!("{}: Starting as foreground", glommio::executor().id());

              let mut connection = capture.connect_work_pool(false).await.unwrap();
              let result = foreground(WorkPoolConnection { inner: connection.inner.clone() }).await;
              connection.inner.wait_until_none_outstanding().await.expect("Failed to wait for all outstanding tasks");
              connection.inner.close().await.expect("Failed to close work pool connection");
              trace!("{}: Foreground thread terminating", glommio::executor().id());
              result
            } else {
                trace!("{}: Starting as background", glommio::executor().id());

              capture.join_work_pool_as_background().await.unwrap_or_else(|e| panic!("{}: Background work pool thread failed: {:#?}", glommio::executor().id(), e));
              trace!("{}: Background thread terminating", glommio::executor().id());
              background()
            }
          })).map_err(map_glommio_error)?.join_all();
        trace!("Started work pool has terminated");
        results
            .into_iter()
            .map(|r| r.map_err(map_glommio_error))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn spin_up_down_with_outstanding_messages() {
        let num_foreground = 1;
        let num_background = 1;
        let foreground_started = Arc::new(AtomicUsize::new(0));
        let background_finished = Arc::new(AtomicUsize::new(0));
        let background_pongs_received = Arc::new(AtomicUsize::new(0));

        let foreground_set_foreground_started = foreground_started.clone();
        let background_check_foreground_started = foreground_started.clone();
        let async_background_finished = background_finished.clone();
        let background_set_background_finished = background_finished.clone();
        let foreground_set_pong_received = background_pongs_received.clone();

        let work_pool = WorkPool::new(num_foreground, num_background, 1);
        work_pool
            .start(
                move |pool| async move {
                    let mut pool = pool.inner;
                    foreground_set_foreground_started.fetch_add(1, Ordering::Release);
                    assert_eq!(
                        async_background_finished.load(Ordering::Relaxed),
                        0,
                        "Background thread can't have finished before the foreground thread"
                    );
                    let sent = Rc::new(glommio::sync::Semaphore::new(0));
                    let spawned_sent = sent.clone();
                    let received = Rc::new(glommio::sync::Semaphore::new(0));
                    let spawned_pool = pool.clone();
                    let spawned_task = glommio::executor()
                        .spawn_local(async move {
                            let mut response_future =
                                Box::pin(spawned_pool.distribute_work(InterThreadRequest::Ping {
                                    pong_delay: Some(Duration::from_millis(100)),
                                }));

                            // Make sure we send it out.
                            assert!(futures::poll!(&mut response_future).is_pending());

                            // Now signal the semaphore.
                            spawned_sent.signal(1);

                            match response_future.await {
                                Ok(InterThreadResponse::Pong) => {
                                    foreground_set_pong_received.fetch_add(1, Ordering::Release)
                                }
                                e => panic!(
                                    "{}: Did not receive expected pong result. Got: {:#?}",
                                    glommio::executor().id(),
                                    e
                                ),
                            };
                            println!("{}: Received pong response", glommio::executor().id());
                        })
                        .detach();
                    {
                        let _guard = sent
                            .acquire_permit(1)
                            .await
                            .expect("Failed to acquire semaphore");
                    }
                    sent.close();
                    println!("{}: Waiting on oustanding work", glommio::executor().id());
                    pool.wait_until_none_outstanding()
                        .await
                        .expect("Failed to wait for outstanding work");
                    println!("{}: No outstanding requests", glommio::executor().id());

                    assert_eq!(received.available(), 1);
                    assert!(futures::poll!(spawned_task).is_ready());
                },
                move || {
                    assert_eq!(
                        background_check_foreground_started.load(Ordering::Relaxed),
                        num_foreground,
                        "Foreground thread must have finished for this callback to be invoked"
                    );
                    background_set_background_finished.fetch_add(1, Ordering::Release);
                },
            )
            .unwrap();
        assert_eq!(foreground_started.load(Ordering::Relaxed), num_foreground);
        assert_eq!(background_finished.load(Ordering::Relaxed), num_background);
    }
}

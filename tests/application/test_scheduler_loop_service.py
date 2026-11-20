def test_scheduler_loop_rolls_back_on_unexpected_assignment_error() -> None:
    """
    If worker assignment fails with an error other than
    NoAvailableNodeError, the scheduler must still unschedule the job
    and release node capacity before propagating the error so the job
    is not stranded in SCHEDULED with no lease.
    """
    node = make_node(
        id=NodeId.new(),
        capacity=ResourceRequirements(
            cpu_cores=16,
            memory_mib=32768,
            vram_mib=16384,
        ),
    )
    initial_available = node.available

    job = make_job(
        id=JobId.new(),
        resources=ResourceRequirements(
            cpu_cores=4,
            memory_mib=4096,
            vram_mib=2048,
        ),
    )
    job.queue()

    jobs = InMemoryJobRepository([job])
    nodes = InMemoryNodeRepository([node])
    events = InMemoryEventRepository()
    record_job_events_service = RecordJobEventsService(
        event_repository=events,
    )

    class _FailingAssignWorkerService:
        def execute(self, _job) -> None:
            raise RuntimeError("simulated worker/lease failure")

    service = SchedulerLoopService(
        jobs,
        nodes,
        Scheduler(),
        assign_worker_service=_FailingAssignWorkerService(),  # type: ignore[arg-type]
        record_job_events_service=record_job_events_service,
    )

    with pytest.raises(RuntimeError, match="simulated worker/lease failure"):
        service.execute()

    assert job.status == JobStatus.QUEUED
    assert job.assigned_node_id is None
    assert node.available == initial_available
    event_types = [event.event_type for event in events.list(DEFAULT_TENANT_ID)]
    assert event_types == ["JobScheduled", "JobUnscheduled"]

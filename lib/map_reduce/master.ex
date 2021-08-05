defmodule MapReduce.Master do
  use GenServer

  require Logger

  @type master_state :: %{
    filenames: list(String.t()),
    map_files: map(),
    reduce_files: map(),
    pending_jobs: map(),
    backlog_jobs: list(MapReduce.Job.job|nil)
  }


  #############################

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, :master})
  end

  def connect(worker_name) do
    GenServer.cast({:global, :master}, {"connect_worker", worker_name})
  end

  def get_job(worker_name) do
    GenServer.call({:global, :master}, {"get_job", worker_name})
  end

  def job_done(worker_name, job) do
    GenServer.cast({:global, :master}, {"job_done", worker_name, job})
  end

  #############################

  @spec init(any) :: {:ok, master_state()}
  def init(_opts) do
    Logger.info("Master started")
    files = get_files()
    map_files = files |> get_initial_map_file_map

    {:ok, %{
      filenames: files,
      map_files: map_files,
      reduce_files: %{},
      pending_jobs: %{},
      backlog_jobs: []
    }}
  end

  def handle_call({"get_job", worker_name},_from, state) do
    Logger.info("WORKER #{inspect(worker_name)} to asking for job ")
    # TODO: get a job
    {job, state} =
      case find_job(worker_name, state) do
        {:nil, state} ->
          {:nil, state}
        {job, state} ->
          state = Map.put(state, worker_name, job)
          # TODO: assign job to worker
          # MapReduce.Worker.assign_job(worker_name, job)

          # TODO: ping worker

          {job, state}
      end
    {:reply, job, state}
  end

  def handle_cast({"connect_worker", worker_name}, state) do
    Logger.info("WORKER to MASTER #{inspect(worker_name)}")
    worker_name |> IO.inspect(label: "worker name")
    # TODO: register_conf
    MapReduce.Worker.register_confirmation(worker_name)
    {:noreply, state}
  end


  def handle_cast({"job_done", _worker, job}, state) do
    # TODO: job cleanup
    state = post_job_done(job, state)
    {:noreply, state}
  end

  ###############################

  defp get_files() do
    "priv/resources"
    |> File.ls!()
  end

  defp get_initial_map_file_map(files) do
    files
      |> Enum.map(fn file -> {file, true} end)
      |> Map.new()
  end

  defp find_job(worker, state) do
    with {:not_found, state} <- from_backlog(state),
    {:not_found, state} <- map_jobs(state),
    {:not_found, state} <- reduce_jobs(state) do
      Logger.info("No jobs")
      {:nil, state}
    else
      {:found, :backlog, job, state} ->
        Logger.info("found backlog job")
        job = %{job | worker: worker}
        {job, state}
      {:found, :map, file, state} ->
        # TODO: make map job
        Logger.info("found map job")
        job = MapReduce.Job.new(:map, file, worker)
        {job, state}
      {:found, :reduce, file, state} ->
        # TODO: make reduce job
        Logger.info("found reduce job")
        job = MapReduce.Job.new(:reduce, file, worker)
        {job, state}
    end
    # job preference
    #  - Backlog
    #  - Map
    #  - Reduce
    # no job, do nothing
  end

  defp from_backlog(%{backlog_jobs: []} = state), do: {:not_found, state}
  defp from_backlog(%{backlog_jobs: [h|t]} = state) do
    {:found,:backlog, h, %{state| backlog_jobs: t}}
  end

  defp map_jobs(%{map_files: map_files}=state) do
    cond do
      Enum.empty?(map_files) ->
        {:not_found, state}
      true ->
        {job_file, rest} =
          map_files
          |> Map.keys
          |> List.first
          |> then(&({ &1, Map.drop(map_files, [&1]) }))
        {:found, :map, job_file, %{state| map_files: rest}}
    end
  end

  defp reduce_jobs(%{reduce_files: reduce_files}=state) do
    cond do
      Enum.empty?(reduce_files) ->
        {:not_found, state}
      true ->
        {job_file, rest} =
          reduce_files
          |> Map.keys
          |> List.first
          |> then(&({ &1, Map.drop(reduce_files, [&1]) }))
        {:found,:reduce, job_file, %{state| reduce_files: rest}}
    end
  end

  defp post_job_done(%{type: :map} = job, state) do
    filename = job.filename
    worker = job.worker
    pending_jobs = Map.drop(state.pending_jobs, [worker])
    reduce_files = Map.put(state.reduce_files, filename, true)
    %{state| reduce_files: reduce_files, pending_jobs: pending_jobs}
  end

  defp post_job_done(%{type: :reduce} = job, state) do
    worker = job.worker
    pending_jobs = Map.drop(state.pending_jobs, [worker])
    %{state| pending_jobs: pending_jobs}
  end

end

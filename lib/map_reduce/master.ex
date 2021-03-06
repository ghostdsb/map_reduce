defmodule MapReduce.Master do
  use GenServer

  require Logger

  @type master_state :: %{
    filenames: list(String.t()),
    map_files: list({String.t(), integer()}),
    reduce_files: list(String.t()),
    pending_jobs: map(),
    backlog_jobs: list(MapReduce.Job.job)
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
    map_files = files |> get_initial_mapfiles_map

    {:ok, %{
      filenames: files,
      map_files: map_files,
      reduce_files: [],
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
          state
          |> check_job_completion()
          {:nil, state}
        {job, state} ->
          state = %{state | pending_jobs: Map.put(state.pending_jobs, worker_name, job)}

          # TODO: ping worker
          Process.send_after(self(), {"ping_worker", worker_name}, 10000)

          {job, state}
      end
    {:reply, job, state}
  end

  def handle_cast({"connect_worker", worker_name}, state) do
    Logger.info("WORKER to MASTER #{inspect(worker_name)}")
    # TODO: register_conf
    MapReduce.Worker.register_confirmation(worker_name)
    {:noreply, state}
  end


  def handle_cast({"job_done", _worker, job}, state) do
    # TODO: job cleanup
    state = post_job_done(job, state)
    {:noreply, state}
  end

  def handle_info({"ping_worker", worker_name}, state) do
    # TODO: ping worker
    # Logger.info("Checking if #{inspect(worker_name)} is alive")
    state =
      case :global.whereis_name(worker_name) do
        :undefined ->
          job = Map.get(state.pending_jobs, worker_name)
          case job do
            nil ->
              state
            _ ->
              Logger.info("#{inspect(worker_name)} not alive, putting job to backlog")
              %{state |
              backlog_jobs: [job | state.backlog_jobs],
              pending_jobs: Map.drop(state.pending_jobs, [worker_name])
            }
          end
        _ ->
          # Logger.info("#{inspect(worker_name)} is alive")
          Process.send_after(self(), {"ping_worker", worker_name}, 10000)
          state
    end
    {:noreply, state}
  end

  ###############################

  defp get_files() do
    "priv/resources"
    |> File.ls!()
  end

  defp get_initial_mapfiles_map(files) do
    files
      |> Enum.with_index()
  end

  defp find_job(worker, state) do
    # job preference
    #  - Backlog
    #  - Map
    #  - Reduce
    # no job, do nothing
    with {:not_found, state} <- from_backlog(state),
    {:not_found, state} <- map_jobs(state),
    {:not_found, state} <- reduce_jobs(state) do
      # Logger.info("No jobs")
      {:nil, state}
    else
      {:found, :backlog, job, state} ->
        Logger.info("found backlog job")
        job = %{job | worker: worker}
        {job, state}
      {:found, :map, {file, idx}, state} ->
        # TODO: make map job
        Logger.info("found map job")
        job = MapReduce.Job.new(:map, file, idx, worker)
        {job, state}
      {:found, :reduce, file, state} ->
        # TODO: make reduce job
        Logger.info("found reduce job")
        job = MapReduce.Job.new(:reduce, file, worker)
        {job, state}
    end
  end

  defp from_backlog(%{backlog_jobs: []} = state), do: {:not_found, state}
  defp from_backlog(%{backlog_jobs: [h|t]} = state) do
    {:found,:backlog, h, %{state| backlog_jobs: t}}
  end

  defp map_jobs(%{map_files: []}=state), do: {:not_found, state}
  defp map_jobs(%{map_files: [h|rest]}=state), do: {:found, :map, h, %{state| map_files: rest}}

  defp reduce_jobs(%{reduce_files: []}=state), do: {:not_found, state}
  defp reduce_jobs(%{reduce_files: [h|rest]}=state), do: {:found, :reduce, h, %{state| reduce_files: rest}}

  defp post_job_done(%{type: :map} = job, state) do
    file_id = job.file_id
    worker = job.worker
    n_workers = Application.get_env(:map_reduce, :n_workers)
    intermediate_files =
      file_id
      |> List.duplicate(n_workers)
      |> Enum.with_index()
      |> Enum.map(fn {id, index} -> "#{id}_#{index}.txt" end)

    pending_jobs = Map.drop(state.pending_jobs, [worker])
    reduce_files = state.reduce_files ++ intermediate_files
    %{state| reduce_files: reduce_files, pending_jobs: pending_jobs}
  end

  defp post_job_done(%{type: :reduce} = job, state) do
    worker = job.worker
    pending_jobs = Map.drop(state.pending_jobs, [worker])
    %{state| pending_jobs: pending_jobs}
  end

  defp check_job_completion(state) do
    jobs = Enum.count(state.pending_jobs) +
      Enum.count(state.backlog_jobs) +
      Enum.count(state.map_files) +
      Enum.count(state.reduce_files)
    case jobs do
      0 ->
        # MapReduce.check_distributed_mapreduce()
        Logger.info("ALL JOBS DONE!")

      _ ->
        Logger.info("waiting for job completion")
    end
  end

end

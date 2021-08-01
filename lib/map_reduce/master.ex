defmodule MapReduce.Master do
  use GenServer

  require Logger

  @type master_state :: %{
    jobs: list(MapReduce.Job.job()),
    n_map_jobs: integer(),
    files: list(String.t()),
    n_reduce_jobs: integer(),
    intermediate: list(any())
  }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, :master})
  end

  def peek_state do
    GenServer.call({:global, :master}, "peek")
  end

  def register_worker(worker_name) do
    GenServer.call({:global, :master}, {"register", worker_name})
  end

  def assign_job(worker_name) do
    GenServer.call({:global, :master}, {"assign", worker_name})
  end

  def worker_ready(worker_name) do
    GenServer.cast({:global, :master}, {"worker_ready", worker_name})
  end

  def finished_job(job) do
    GenServer.cast({:global, :master}, {"finished_job", job})
  end

  @spec init(any) :: {:ok, master_state()}
  def init(_opts) do
    Process.flag(:trap_exit, true)
    IO.inspect("Master started")
    files =
      File.ls!("priv/resources")
      |> Enum.with_index()
    {:ok, %{
      files: files,
      n_map_jobs: files |> Enum.count,
      n_reduce_jobs: 0,
      intermediate: [],
      jobs: []
    }
    }
  end

  def handle_call("peek", _from, state) do
    {:reply, state, state}
  end

  def handle_call({"register", worker_name}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({"assign", worker_name}, _from, state) do
    # find job
    {job, rest, type} = find_job(state, worker_name)
    state = case type do
      :map -> %{state| files: rest}
      :reduce -> %{state| intermediate: rest}
    end
    {:reply, job, state}
  end


  def handle_cast({"worker_ready", worker_name}, state) do
    {job, rest, type} = find_job(state, worker_name)
    IO.inspect("#{worker_name} asking for work")
    case type do
      :nil ->
        IO.inspect("no more jobs to be done #{worker_name}")
        {:noreply, state}
      _ ->
        state = case type do
          :map ->
            %{state|
            files: rest
          }
          :reduce ->
            %{state|
            intermediate: rest
          }
        end
        MapReduce.Worker.do_job(worker_name, job)
        {:noreply, state}
    end
  end

  def handle_cast({"finished_job", job}, state) do
    %{type: type, worker: worker, resource: resource} = job
    state = case type do
      :map ->
        {res, _idx} = resource
        IO.inspect(label: "map job of #{res} done by #{worker}")

        %{state|
        # files: state.files -- [res],
        intermediate: [resource| state.intermediate],
        n_map_jobs: state.n_map_jobs-1,
        n_reduce_jobs: state.n_reduce_jobs+1,
      }
    :reduce ->
      {res, _idx} = resource
      IO.inspect(label: "reduce job of #{res} done by #{worker}")
      state
    end
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    reason
    |> IO.inspect(label: "exit")
    {:noreply, state}
  end

  def handle_info(message, state) do
    message |> Logger.info(label: "handleinfo")
    {:noreply, state}
  end

  defp find_job(state, worker_name) do
    cond do
      state.n_map_jobs > 0 ->
        {job_file, rest} = state.files |> get_map_file()
        case job_file do
          :nil ->
            {:nil, [], :nil}
          _ ->
            job = MapReduce.Job.new(:map, job_file, worker_name)
            {job, rest, :map}
        end
      true ->
        # [done_by|rest] = state.intermediate
        {file, rest} = state.intermediate |> get_intermediate()
        case file do
          :nil ->
            {:nil, [], :nil}
          _ ->
            job = MapReduce.Job.new(:reduce, file, worker_name)
            {job, rest, :reduce}
        end
    end
    # {job,rest}
  end

  defp get_map_file([]), do: {:nil, []}
  defp get_map_file([file|rest]), do: {file, rest}
  defp get_intermediate([]), do: {:nil, []}
  defp get_intermediate([file|rest]), do: {file, rest}
end

defmodule MapReduce.Worker do
  use GenServer

  require Logger

  @type worker_state :: %{
    name: atom(),
    status: :busy | :idle
  }

  ############################

  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name) |> IO.inspect(label: "opts")
    GenServer.start_link(__MODULE__, name, name: {:global, name})
  end

  def register_confirmation(worker_name) do
    GenServer.cast({:global, worker_name}, "connected")
  end

  def get_status(worker_name) do
    GenServer.call({:global, worker_name}, "get_status")
  end


  #############################

  @spec init(keyword) :: {:ok, worker_state()}
  def init(name) do
    Logger.info("Worker #{inspect(name)} started")
    Process.send_after(self(), "connect_to_master", 2000)

    {:ok, %{
      name: name,
      status: :idle
    }}
  end


  def handle_call("get_status", _from, state) do
    {:reply, state.status, state}
  end

  def  handle_cast("connected", state) do
    Process.send_after(self(), "ask_for_job", 1000)
    {:noreply, state}
  end

  def handle_cast({"execute_job", job}, state) do
    Process.send_after(self(), {"start_job", job}, 1000)
    state = %{state| status: :busy}
    {:noreply, state}
  end

  def handle_info("connect_to_master", state) do
    Logger.info("connecting to master")
    case Node.connect(:master@titan) do
      true ->
        # connect_master
        Process.send_after(self(), "connect_delay", 1000)
      _ ->
        Logger.info("could not connect to master")
    end
    {:noreply, state}
  end

  def handle_info("connect_delay", state) do
    Logger.info("CONNECTED TO MASTER")
    MapReduce.Master.connect(state.name)
    {:noreply, state}
  end

  def handle_info({"start_job", job}, state) do
    # TODO: execute job
    job |> execute_job()

    # TODO: job done
    MapReduce.Master.job_done(state.name, job)
    Process.send_after(self(), "ask_for_job", 1000)
    state = %{state| status: :idle}
    {:noreply, state}
  end

  def handle_info("ask_for_job", state) do
    Logger.info("ASKING FOR JOB!")
    # TODO: ask for job
    state = case MapReduce.Master.get_job(state.name) do
      :nil ->
        :init.stop()
        Logger.info("jobs completed")
        state
      job ->
        Logger.info("doing #{inspect(job)}")
        Process.send_after(self(), {"start_job", job}, 1000)
        %{state| status: :busy}
    end
    {:noreply, state}
  end

  ################################

  defp execute_job(%{type: :map}=job) do
    filename = job.filename
    file_id = job.file_id
    loc = "priv/resources/#{filename}"
    content = File.read!(loc)


    WordCount.mapper(loc, content)
    |> Enum.sort()
    |> Enum.each(fn {key, val} -> write_to_intermediate_phase(file_id, key, val) end)
  end

  defp execute_job(%{type: :reduce}=job) do
    filename = job.filename
    output_loc = String.split(filename, "_") |> List.first()
    loc = "d-intermediate/#{filename}"
    content = File.read!(loc)

    io_device =
      "d-output/#{output_loc}.txt"
      |> File.open!([:write, :append, :utf8])

    data = content
      |> String.split("\n")
      |> Enum.filter(fn x-> x !== "" end)
      |> Enum.map(fn x -> String.split(x, ":") end)
      |> Enum.chunk_by(fn [k,_v] -> k end)

    data
    |> Enum.map(fn chunk ->
      [[key, _val]|_] = chunk
      count = WordCount.reducer(key, chunk)

      IO.write(io_device, "#{key} #{count}\n")
    end)

  end

  defp write_to_intermediate_phase(file_id, key, val) do
    n_workers = Application.get_env(:map_reduce, :n_workers)
    partition_id = rem(:erlang.phash2(key), n_workers)
    io_device =
      "d-intermediate/#{file_id}_#{partition_id}.txt"
      |> File.open!([:write, :append, :utf8])
    IO.write(io_device, "#{key}:#{val}\n")
      File.close(io_device)
  end

end

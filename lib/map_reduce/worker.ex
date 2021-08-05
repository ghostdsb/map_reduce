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

  # def assign_job(worker_name, job) do
  #   GenServer.cast({:global, worker_name}, {"execute_job", job})
  # end

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

  def  handle_cast("connected", state) do
    Process.send_after(self(), "ask_for_job", 1000)
    # Logger.info("ASKING FOR JOB!")
    # # TODO: ask for job
    # job = MapReduce.Master.get_job(state.name) |> IO.inspect(label: "THIS JOB")
    # Process.send_after(self(), {"start_job", job}, 1000)
    # state = %{state| status: :busy}
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
        Logger.info("jobs completed")
        state
      job ->
        Logger.info("doing #{inspect(job)}")
        Process.send_after(self(), {"start_job", job}, 1000)
        state = %{state| status: :busy}
    end
    {:noreply, state}
  end

  ################################

  defp execute_job(%{type: :map}=job) do
    filename = job.filename
    loc = "priv/resources/#{filename}"
    content = File.read!(loc)

    data =
      WordCount.mapper(loc, content)
      |> Enum.sort()

    i_data =
      data
      |> Enum.map(fn {key, val} -> "#{key}:#{val}\n"end)

    io_device =
      "mr-intermediate-#{filename}"
      |> File.open!([:write, :append, :utf8])

    IO.write(io_device, i_data)
  end

  defp execute_job(%{type: :reduce}=job) do
    filename = job.filename
    loc = "mr-intermediate-#{filename}"
    content = File.read!(loc)

    io_device =
      "mr-final-#{filename}"
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

end

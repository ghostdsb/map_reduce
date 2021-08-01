defmodule MapReduce.Worker do
  use GenServer

  require Logger

  @type worker_state :: %{
    name: atom(),
    status: :busy | :idle,
    output: any()
  }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, Keyword.fetch!(opts, :name)})
  end

  def get_output(worker_name) do
    GenServer.call({:global, worker_name}, "get_output")
  end

  def do_job(worker_name, job) do
    GenServer.cast({:global, worker_name}, {"do_job", job})
  end

  @spec init(keyword) :: {:ok, worker_state()}
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    IO.inspect("Worker #{name} started")
    connect_master_node(name)
    {:ok, %{name: name, status: :idle, output: nil}}
  end

  def handle_call("get_output", _from, state) do
    {:reply, state.output, state}
  end

  def handle_cast({"do_job",job}, state) do
    output =
      job
      |> perform_job()
      # |> IO.inspect(label: "worker-result")
    state = %{state | output: output}
    MapReduce.Master.finished_job(job)
    ask_for_work(state.name)
    {:noreply, state}
  end

  def handle_info("register", state) do
    MapReduce.Master.register_worker(state.name)
    ask_for_work(state.name)
    {:noreply, state}
  end

  def handle_info("ask_for_work", state) do
    MapReduce.Master.worker_ready(state.name)
    # ask_for_work(state.name)
    {:noreply, state}
  end

  defp connect_master_node(_name) do
    cond do
      Node.connect(:master@titan) ->
        IO.inspect(label: "master connection")
        Process.send_after(self(), "register", 5000)
      true ->
        Logger.info("Error connecting to master")
    end
  end

  defp ask_for_work(name) do
    IO.inspect("#{name} asking for work")
    Process.send_after(self(), "ask_for_work", 3000)
  end

  defp perform_job(job) do
    perform_job(job, job.type)

  end

  defp perform_job(job, :map) do
    {filename, _idx} = job.resource
    IO.inspect(label: "doing map job of #{filename}")
    loc = "priv/resources/#{filename}"
    content = File.read!(loc)
    data = WordCount.mapper(loc, content)
    |> Enum.sort()

    i_data = data
    |> Enum.map(fn {key, val} -> "#{key}:#{val}\n"end)

    io_device =
      "mr-intermediate-#{filename}"
      |> File.open!([:write, :append, :utf8])
    IO.write(io_device, i_data)

    data
  end
  # defp perform_job(job, :map) do
  #   {filename, _idx} = job.resource
  #   loc = "priv/resources/#{filename}"
  #   content = File.read!(loc)
  #   data = WordCount.mapper(loc, content)
  #   |> Enum.sort()
  #   |> Enum.chunk_by(fn {k,_v} -> k end)

  #   i_data = data
  #   |> Enum.map(fn {key, val} -> "#{key}:#{value}"end)
  #   |> Enum.reduce("", fn {key, val}, acc ->

  #   end)
  #   io_device =
  #     "mr-intermediate-#{filename}"
  #     |> File.open!([:write, :append, :utf8])
  #   IO.write(io_device, data)

  #   data
  # end

  defp perform_job(job, :reduce) do
    {resource_filename, _idx} =
      job.resource
    IO.inspect(label: "doing reduce job of #{resource_filename}")
    loc = "mr-intermediate-#{resource_filename}"
    content = File.read!(loc)

    io_device =
      "mr-final-#{resource_filename}"
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
  # defp perform_job(job, :reduce) do
  #   worker_name =
  #     job.resource

  #   data = worker_name
  #     |> MapReduce.Worker.get_output()

  #   data
  #   |> Enum.map(fn chunk ->
  #     [{key, _val}|_] = chunk
  #     {key, WordCount.reducer(key, chunk)}
  #   end)

  # end
end

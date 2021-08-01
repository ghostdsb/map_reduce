defmodule MapReduce.Application do
  use Application

  def start(_type, _arg) do
    Application.get_all_env(:map_reduce)
    |> IO.inspect(label: "env")

    mode = Application.get_env(:map_reduce, :mr_mode, :master)
    name = Application.get_env(:map_reduce, :mr_name, :master)

    children = [get_child_process(mode, name)]
    options = [strategy: :one_for_one, name: MapReduce.Supervisor]

    Supervisor.start_link(children, options)
  end

  defp get_child_process(:master, :master) do
    {MapReduce.Master, [mode: :master, name: :master]}
  end

  defp get_child_process(:worker, name) do
    {MapReduce.Worker, [mode: :worker, name: name]}
  end
end

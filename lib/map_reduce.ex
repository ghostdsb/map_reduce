defmodule MapReduce do

  require Logger

  def check_distributed_mapreduce do
    "priv/resources"
    |> File.ls!()
    |> Enum.each(fn filename -> filename |> check_similarity() end)
  end

  defp check_similarity(filename) do
    seq = "mr-seq-#{filename}"
    dis = "mr-final-#{filename}"
    System.cmd("cmp", [seq, dis])
    |> then(&log_result(filename, &1))
  end

  defp log_result(filename, {"", 0}), do: Logger.info(":ok #{filename}")
  defp log_result(filename, {_, _}), do: Logger.info(":error #{filename}")
end

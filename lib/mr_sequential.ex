defmodule MrSequential do

  def run(application, dirname) do
    intermediate =
      dirname
      |> File.ls!()
      |> Enum.reduce([], fn filename, itr_data ->
        loc = "#{dirname}/#{filename}"
        content = File.read!(loc)
        itr_data ++ application.mapper(loc, content)
      end)
      |> Enum.sort()
      |> Enum.chunk_by(fn {k,_v} -> k end)

# intermediate
# [      [{"ABOLITIONIST", 1}],
#       [{"ABOMINABLE", 1}],
#       [{"ABOMINATION", 1}],
#       [{"ABORDE", 1}],
#       [{"ABORTION", 1}],
#       [{"ABORTIVE", 1}],
#       [{"ABOUND", 1}],
#       [{"ABOUNDING", 1}],
#       [
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", 1},
#         {"ABOUT", ...},
#         {...},
#         ...
#       ],
# ]

    io_device =
      "mr-out-0"
      |> File.open!([:write, :append, :utf8])

    process_intermediate(intermediate, application, io_device)

    File.close(io_device)
  end

  defp process_intermediate([], _application, _io_device), do: []
  defp process_intermediate([h|t], application, io_device) do
    [{key, _value}|_] = h
    output = application.reducer(key, h)
    IO.write(io_device, "#{key} #{output}\n")
    process_intermediate(t, application, io_device)
  end
end

defmodule WordCount do

  def mapper(_filename, content) do
    content
    |> String.split(~r{[^a-zA-Z]})
    |> Enum.filter(fn word -> word != "" end)
    |> Enum.map(fn word -> {String.upcase(word), 1} end)
  end

  def reducer(_key, values) do
    values
    |> Enum.count()
  end
end

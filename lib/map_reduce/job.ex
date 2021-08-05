defmodule MapReduce.Job do

  @type job :: %{
    id: integer(),
    type: :map | :reduce,
    filename: String.t(),
    worker: atom()
  }

  @spec new(any, any, any) :: job()
  def new(type, filename, worker) do
    job_data = %{
      type: type,
      filename: filename,
      worker: worker
    }
    id = :erlang.phash2(job_data)
    Map.put(job_data, :id, id)
  end
end

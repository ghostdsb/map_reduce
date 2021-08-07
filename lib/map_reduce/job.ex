defmodule MapReduce.Job do

  @type job :: %{
    # id: integer(),
    type: :map | :reduce,
    filename: String.t(),
    file_id: number(),
    worker: atom()
  }

  @spec new(any, any, any, any) :: job()
  def new(:map, filename, file_id, worker) do
    # job_data =
      %{
      type: :map,
      filename: filename,
      file_id: file_id,
      worker: worker
    }
    # id = :erlang.phash2(job_data)
    # Map.put(job_data, :id, id)
  end

  @spec new(:reduce, any, any) :: %{filename: any, type: :reduce, worker: any}
  def new(:reduce, filename, worker) do
    # job_data =
      %{
      type: :reduce,
      filename: filename,
      worker: worker
    }
    # id = :erlang.phash2(job_data)
    # Map.put(job_data, :id, id)
  end
end

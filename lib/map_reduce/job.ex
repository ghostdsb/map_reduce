defmodule MapReduce.Job do

  @type job :: %{
    id: integer(),
    type: :map | :reduce,
    resource: String.t() | pid(),
    worker: pid()
  }

  defstruct(
    type: nil,
    resource: nil
  )

  @spec new(any, any, any) :: job()
  def new(type, resource, worker_pid) do
    job_data = %{
      type: type,
      resource: resource,
      worker: worker_pid
    }
    id = :erlang.phash2(job_data)
    Map.put(job_data, :id, id)
  end
end

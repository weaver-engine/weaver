defmodule Weaver do
  @moduledoc """
  Root module and main API of Weaver.

  Entry point for weaving queries based on `Absinthe.Pipeline`.

  Use `run/3` for initial execution, returning a `Weaver.Step.Result`.
  Subsequent steps (the result's `dispatched` and `next` steps) can be executed via `resolve/3`.
  """

  defmodule Ref do
    @moduledoc """
    References a node in the graph using a globally unique `id`.

    Used as placeholder in any graph tuples, such as for storing
    and retrieval in `Weaver.Graph`.
    """

    @enforce_keys [:id]
    defstruct @enforce_keys

    @type t() :: %__MODULE__{
            id: String.t()
          }

    def new(id) when is_binary(id), do: %__MODULE__{id: id}
    def from(obj), do: new(Weaver.Resolvers.id_for(obj))
  end

  defmodule Marker do
    @moduledoc """
    References a position in a timeline where a chunk of previously
    retrieved data starts or ends, used as boundaries for retrieval
    of new records in the timeline.

    Can be stored as meta data together with the actual graph
    data in `Weaver.Graph`.
    """

    @enforce_keys [:type, :ref, :val]
    defstruct @enforce_keys

    @type t() :: %__MODULE__{
            ref: any(),
            val: any(),
            type: :chunk_start | :chunk_end
          }

    def chunk_start(id, val) do
      %__MODULE__{type: :chunk_start, ref: %Ref{id: id}, val: val}
    end

    def chunk_end(id, val) do
      %__MODULE__{type: :chunk_end, ref: %Ref{id: id}, val: val}
    end
  end

  alias Absinthe.Pipeline
  alias Weaver.Absinthe.Middleware.Continue

  @result_phase Weaver.Absinthe.Phase.Document.Result
  @resolution_phase Absinthe.Phase.Document.Execution.Resolution

  def prepare(document, schema, options \\ []) do
    context =
      Keyword.get(options, :context, %{})
      |> Map.put_new(:cache, Keyword.get(options, :cache, nil))
      |> Map.put_new(:refresh, Keyword.get(options, :refresh, true))
      |> Map.put_new(:backfill, Keyword.get(options, :backfill, true))

    options = Keyword.put(options, :context, context)

    pipeline =
      schema
      |> pipeline(options)
      |> Pipeline.without(@resolution_phase)

    case Absinthe.Pipeline.run(document, pipeline) do
      {:ok, %{result: {:validation_failed, errors}}, _phases} ->
        {:error, {:validation_failed, errors}}

      {:ok, blueprint, _phases} ->
        {:ok, blueprint}

      {:error, msg, _phases} ->
        {:error, msg}
    end
  end

  def pipeline(schema, options) do
    options = Keyword.put_new(options, :result_phase, @result_phase)

    Pipeline.for_document(schema, options)
    |> Pipeline.replace(Absinthe.Phase.Document.Result, @result_phase)
  end

  def weave(blueprint, options \\ []) do
    pipeline =
      pipeline(blueprint.schema, options)
      |> Pipeline.from(@resolution_phase)

    case Absinthe.Pipeline.run(blueprint, pipeline) do
      {:ok, %{result: result}, _phases} ->
        result =
          case Weaver.Step.Result.next(result) do
            nil ->
              result

            next_blueprint ->
              continuation = next_blueprint.execution.acc[Continue]

              Weaver.Step.Result.set_next(
                result,
                update_in(
                  blueprint.execution.acc,
                  &Map.put(&1, Continue, continuation)
                )
              )
          end

        {:ok, result}

      {:error, msg, _phases} ->
        {:error, msg}
    end
  end
end

defmodule Weaver.Absinthe do
  @moduledoc """
  Entry point for weaving queries based on `Absinthe.Pipeline`.

  Use `run/3` for initial execution, returning a `Weaver.Step.Result`.
  Subsequent steps (the result's `dispatched` and `next` steps) can be executed via `resolve/3`.
  """
  alias Absinthe.Pipeline

  alias Weaver.Absinthe.Middleware.Continue

  @result_phase Weaver.Absinthe.Phase.Document.Result
  @resolution_phase Absinthe.Phase.Document.Execution.Resolution

  def run(document, schema, options \\ []) do
    context =
      Keyword.get(options, :context, %{})
      |> Map.put_new(:cache, Keyword.get(options, :cache, nil))
      |> Map.put_new(:refresh, Keyword.get(options, :refresh, true))
      |> Map.put_new(:backfill, Keyword.get(options, :backfill, true))

    options = Keyword.put(options, :context, context)

    pipeline =
      schema
      |> pipeline(options)

    case Absinthe.Pipeline.run(document, pipeline) do
      {:ok, %{result: result}, _phases} ->
        {:ok, result}

      {:error, msg, _phases} ->
        {:error, msg}
    end
  end

  def pipeline(schema, options) do
    options = Keyword.put_new(options, :result_phase, @result_phase)

    Pipeline.for_document(schema, options)
    |> Pipeline.replace(Absinthe.Phase.Document.Result, @result_phase)
  end

  def resolve(blueprint, schema, options \\ []) do
    pipeline =
      pipeline(schema, options)
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

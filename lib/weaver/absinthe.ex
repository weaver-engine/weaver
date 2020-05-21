defmodule Weaver.Absinthe do
  alias Absinthe.Pipeline

  alias Weaver.Absinthe.Middleware.Continue

  @result_phase Weaver.Absinthe.Phase.Document.Result
  @resolution_phase Absinthe.Phase.Document.Execution.Resolution

  def run(document, schema, options \\ []) do
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
    pipeline = Pipeline.for_document(schema, options)
    # |> Pipeline.before(@resolution_phase)

    # |> Pipeline.replace(
    #   @resolution_phase,
    #   Weaver.@resolution_phase
    # )
    pipeline
    |> Enum.map(fn
      {mod, _} -> mod
      mod -> mod
    end)
    |> IO.inspect(label: "pipeline")

    pipeline
    |> Pipeline.replace(Absinthe.Phase.Document.Result, @result_phase)
  end

  def resolve(blueprint, schema, options \\ []) do
    # options = Pipeline.options(options)

    pipeline =
      pipeline(schema, options)
      # |> Pipeline.from(Weaver.@resolution_phase)

      |> Pipeline.from(@resolution_phase)

    # blueprint = update_in(blueprint.execution.acc, &Map.put(&1, :resolution, resolution))

    # blueprint =
    #   update_in(blueprint.execution.acc, &Map.delete(&1, Weaver.Absinthe.Middleware.Dispatch))

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

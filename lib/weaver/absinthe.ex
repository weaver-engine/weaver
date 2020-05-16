defmodule Weaver.Absinthe do
  alias Absinthe.Pipeline

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
    Pipeline.for_document(schema, options)
    # |> Pipeline.before(Absinthe.Phase.Document.Execution.Resolution)

    # |> Pipeline.replace(
    #   Absinthe.Phase.Document.Execution.Resolution,
    #   Weaver.Absinthe.Phase.Document.Execution.Resolution
    # )
    |> Pipeline.replace(Absinthe.Phase.Document.Result, Weaver.Absinthe.Phase.Document.Result)
  end

  def resolve(blueprint, schema, options \\ []) do
    # options = Pipeline.options(options)
    pipeline =
      pipeline(schema, options)
      # |> Pipeline.from(Weaver.Absinthe.Phase.Document.Execution.Resolution)

      |> Pipeline.from(Absinthe.Phase.Document.Execution.Resolution)

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
              prev_end_marker = next_blueprint.execution.acc[:please_come_again]

              Weaver.Step.Result.set_next(
                result,
                update_in(
                  blueprint.execution.acc,
                  &Map.put(&1, :please_come_again, prev_end_marker)
                )
              )
          end

        {:ok, result}

      {:error, msg, _phases} ->
        {:error, msg}
    end
  end
end

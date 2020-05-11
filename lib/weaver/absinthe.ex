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
    |> Pipeline.replace(Absinthe.Phase.Document.Result, Weaver.Absinthe.Phase.Document.Result)
  end
end

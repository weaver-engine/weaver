defmodule Weaver.Absinthe.Phase.Document.Result do
  @moduledoc false

  # Produces data fit for external encoding from annotated value tree

  alias Absinthe.{Blueprint, Phase, Type}
  alias Weaver.Step.Result
  alias Weaver.Ref
  use Absinthe.Phase

  @spec run(Blueprint.t() | Phase.Error.t(), Keyword.t()) :: {:ok, map}
  def run(%Blueprint{} = bp, _options \\ []) do
    IO.inspect(bp.execution, limit: 18, label: "execution")

    result =
      case bp.result do
        %{} -> process(bp)
        other -> Result.merge(other, process(bp))
      end

    {:ok, %{bp | result: result}}
  end

  defp process(blueprint) do
    case blueprint.execution do
      %{validation_errors: [], result: nil} ->
        data(nil, %{value: nil}, Result.empty())

      %{validation_errors: [], result: result} ->
        data(nil, result, Result.empty())
        |> IO.inspect(label: "result", limit: 19)

      %{validation_errors: errors} ->
        {:validation_failed, errors}
    end
  end

  defp data(parent, %{errors: [_ | _] = field_errors, emitter: emitter}, result) do
    Result.add_errors(
      result,
      Enum.map(field_errors, &{Ref.from(parent), field_name(emitter), &1})
    )
  end

  # Leaf
  # defp data(nil, _, result) do
  #   result
  # end

  defp data(parent, %Absinthe.Blueprint.Result.Leaf{value: nil, emitter: emitter}, result) do
    Result.add_data(result, {Ref.from(parent), field_name(emitter), nil})
  end

  defp data(_parent, %{value: nil}, result) do
    result
  end

  defp data(parent, %{value: value, emitter: emitter}, result) do
    value =
      case Type.unwrap(emitter.schema_node.type) do
        %Type.Scalar{} = schema_node ->
          Type.Scalar.serialize(schema_node, value)

        %Type.Enum{} = schema_node ->
          Type.Enum.serialize(schema_node, value)
      end

    Result.add_data(result, {Ref.from(parent), field_name(emitter), value})
  end

  # Object
  defp data(nil, %{fields: fields, root_value: obj}, result) when obj == %{} do
    field_data(nil, fields, result)
  end

  defp data(nil, %{fields: fields, root_value: obj}, result) do
    field_data(obj, fields, result)
  end

  defp data(parent, %{fields: fields, emitter: emitter, root_value: obj}, result) do
    result = Result.add_relation_data(result, {Ref.from(parent), field_name(emitter), [obj]})
    field_data(obj, fields, result)
  end

  # List
  defp data(parent, %{values: values}, result) do
    Enum.reduce(values, result, &data(parent, &1, &2))
  end

  defp field_data(_parent, [], result), do: result

  defp field_data(parent, [%Absinthe.Resolution{} = res | fields], result) do
    result = Result.dispatch(result, res)
    field_data(parent, fields, result)
  end

  defp field_data(parent, [field | fields], result) do
    result = data(parent, field, result)
    field_data(parent, fields, result)
  end

  defp field_name(%{alias: nil, name: name}), do: name
  defp field_name(%{alias: name}), do: name
  defp field_name(%{name: name}), do: name
end

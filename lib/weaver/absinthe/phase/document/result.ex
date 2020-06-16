defmodule Weaver.Absinthe.Phase.Document.Result do
  @moduledoc """
  Produces data fit for streaming from annotated value tree.

  Sets a `Weaver.Step.Result` as result.
  """

  # credo:disable-for-this-file Credo.Check.Consistency.ParameterPatternMatching

  alias Absinthe.{Blueprint, Phase, Type}
  alias Absinthe.Blueprint.Result.Leaf
  alias Weaver.Absinthe.Middleware.{Continue, Dispatch}
  alias Weaver.Step.Result
  alias Weaver.Ref
  use Absinthe.Phase

  @spec run(Blueprint.t() | Phase.Error.t(), Keyword.t()) :: {:ok, map}
  def run(%Blueprint{} = bp, _options \\ []) do
    {:ok, %{bp | result: process(bp)}}
  end

  defp process(blueprint) do
    path =
      case blueprint.execution.acc do
        %{resolution: res} -> Enum.reduce(res, [], fn obj, path -> [field_name(obj) | path] end)
        _ -> []
      end

    case blueprint.execution do
      %{validation_errors: [], result: nil} ->
        data(path, nil, %{value: nil}, Result.empty())

      %{validation_errors: [], result: result} ->
        meta = Map.get(blueprint.execution.acc, :meta, [])

        result =
          data(path, nil, result, Result.empty())
          |> Result.add_meta(meta)

        Map.get(blueprint.execution.acc, Dispatch, [])
        |> Enum.reduce(result, fn resolution, result ->
          # only keep :resolution for dispatched steps
          blueprint = put_in(blueprint.execution.acc, %{resolution: resolution})
          Result.dispatch(result, blueprint)
        end)
        |> Result.set_next(blueprint.execution.acc[Continue] && blueprint)

      %{validation_errors: errors} ->
        {:validation_failed, errors}
    end
  end

  defp data([], parent_ref, %{errors: [_ | _] = field_errors, emitter: emitter}, result) do
    Result.add_errors(
      result,
      Enum.map(field_errors, &{parent_ref, field_name(emitter), &1})
    )
  end

  # Leaf
  defp data(path, parent_ref, %Leaf{value: nil, emitter: emitter} = field, result) do
    if on_path?(field, path) do
      Result.add_data(result, {parent_ref, field_name(emitter), nil})
    else
      result
    end
  end

  defp data(_path, _parent, %{value: nil}, result) do
    result
  end

  defp data(path, parent_ref, %{value: value, emitter: emitter} = field, result) do
    if on_path?(field, path) do
      value =
        case Type.unwrap(emitter.schema_node.type) do
          %Type.Scalar{} = schema_node ->
            Type.Scalar.serialize(schema_node, value)

          %Type.Enum{} = schema_node ->
            Type.Enum.serialize(schema_node, value)
        end

      Result.add_data(result, {parent_ref, field_name(emitter), value})
    else
      result
    end
  end

  # Object
  defp data(path, nil, %{fields: fields, root_value: obj} = field, result) when obj == %{} do
    field_data(next_path(field, path), nil, fields, result)
  end

  defp data(path, nil, %{fields: fields} = field, result) do
    field_data(next_path(field, path), to_ref(field), fields, result)
  end

  defp data(path, parent_ref, %{fields: fields, emitter: emitter} = field, result) do
    next_path = next_path(field, path)

    if next_path do
      result =
        if next_path == [] do
          Result.add_relation_data(result, {parent_ref, field_name(emitter), [to_ref(field)]})
        else
          result
        end

      field_data(next_path, to_ref(field), fields, result)
    else
      result
    end
  end

  # List
  defp data(path, parent_ref, %{values: values} = field, result) do
    if on_path?(field, path) do
      case path do
        [next, pos | rest] ->
          val = Enum.at(values, pos)
          data([next | rest], parent_ref, val, result)

        _ ->
          Enum.reduce(values, result, &data(path, parent_ref, &1, &2))
      end
    else
      result
    end
  end

  defp field_data(_path, _parent, [], result), do: result

  defp field_data(path, parent_ref, [%Absinthe.Resolution{} | fields], result) do
    field_data(path, parent_ref, fields, result)
  end

  defp field_data(path, parent_ref, [field | fields], result) do
    result =
      if on_path?(field, path) do
        data(path, parent_ref, field, result)
      else
        result
      end

    field_data(path, parent_ref, fields, result)
  end

  defp field_name(%{alias: nil, name: name}), do: name
  defp field_name(%{alias: name}), do: name
  defp field_name(%{name: name}), do: name
  defp field_name(position) when is_integer(position), do: position

  defp on_path?(%{emitter: emitter}, [field_name | _]) do
    is_nil(field_name) || field_name == field_name(emitter)
  end

  defp on_path?(pos, [other_pos | _]) when is_integer(pos) do
    pos == other_pos
  end

  defp on_path?(_, []), do: true

  defp next_path(field, [_ | next_path] = path) do
    if on_path?(field, path), do: next_path
  end

  defp next_path(_field, []), do: []

  defp to_ref(%{root_value: obj, fields: fields}) do
    case fields do
      [%{emitter: %{alias: "__weaver_id"}, value: id} | _] ->
        %Ref{id: id}

      _ ->
        Ref.from(obj)
    end
  end
end

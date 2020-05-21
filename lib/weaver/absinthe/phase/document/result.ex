defmodule Weaver.Absinthe.Phase.Document.Result do
  @moduledoc false

  # Produces data fit for external encoding from annotated value tree

  alias Absinthe.{Blueprint, Phase, Type}
  alias Absinthe.Blueprint.Result.Leaf
  alias Weaver.Absinthe.Middleware.{Continue, Dispatch}
  alias Weaver.Step.Result
  alias Weaver.Ref
  use Absinthe.Phase

  @spec run(Blueprint.t() | Phase.Error.t(), Keyword.t()) :: {:ok, map}
  def run(%Blueprint{} = bp, _options \\ []) do
    # IO.inspect(bp.execution.acc, limit: 18, label: "execution")

    {:ok, %{bp | result: process(bp)}}
  end

  defp process(blueprint) do
    path =
      case blueprint.execution.acc do
        %{resolution: res} -> Enum.reduce(res, [], fn obj, path -> [field_name(obj) | path] end)
        _ -> []
      end
      |> IO.inspect(label: "path", limit: 10)

    case blueprint.execution do
      %{validation_errors: [], result: nil} ->
        data(path, nil, %{value: nil}, Result.empty())

      %{validation_errors: [], result: result} ->
        IO.inspect(field_name(result.emitter), label: "last result", limit: 15)
        IO.inspect(blueprint.execution.acc, label: "ACC", limit: 12)

        result = data(path, nil, result, Result.empty())

        Map.get(blueprint.execution.acc, Dispatch, [])
        |> Enum.reduce(result, fn resolution, result ->
          # only keep :resolution for dispatched steps
          blueprint = put_in(blueprint.execution.acc, %{resolution: resolution})
          Result.dispatch(result, blueprint)
        end)
        |> Result.set_next(blueprint.execution.acc[Continue] && blueprint)

      # |> IO.inspect(label: "result", limit: 22)

      %{validation_errors: errors} ->
        {:validation_failed, errors}
    end
  end

  defp data([], parent, %{errors: [_ | _] = field_errors, emitter: emitter}, result) do
    Result.add_errors(
      result,
      Enum.map(field_errors, &{Ref.from(parent), field_name(emitter), &1})
    )
  end

  # Leaf
  # defp data(nil, _, result) do
  #   result
  # end

  defp data(path, parent, %Leaf{value: nil, emitter: emitter} = field, result) do
    if on_path?(field, path) do
      Result.add_data(result, {Ref.from(parent), field_name(emitter), nil})
    else
      result
    end
  end

  defp data(_path, _parent, %{value: nil}, result) do
    result
  end

  defp data(path, parent, %{value: value, emitter: emitter} = field, result) do
    if on_path?(field, path) do
      value =
        case Type.unwrap(emitter.schema_node.type) do
          %Type.Scalar{} = schema_node ->
            Type.Scalar.serialize(schema_node, value)

          %Type.Enum{} = schema_node ->
            Type.Enum.serialize(schema_node, value)
        end

      Result.add_data(result, {Ref.from(parent), field_name(emitter), value})
    else
      result
    end
  end

  # Object
  defp data(path, nil, %{fields: fields, root_value: obj} = field, result) when obj == %{} do
    IO.inspect({path, fields}, label: "obj no val/parent", limit: 12)
    field_data(next_path(field, path), nil, fields, result)
  end

  defp data(path, nil, %{fields: fields, root_value: obj} = field, result) do
    IO.inspect({path, obj, fields}, label: "obj+val no parent", limit: 12)
    field_data(next_path(field, path), obj, fields, result)
  end

  defp data(path, parent, %{fields: fields, emitter: emitter, root_value: obj} = field, result) do
    IO.inspect({field_name(emitter), path, on_path?(field, path), obj}, label: "obj+val")

    next_path = next_path(field, path)

    if next_path do
      result =
        if next_path == [] do
          Result.add_relation_data(result, {Ref.from(parent), field_name(emitter), [obj]})
        else
          result
        end

      field_data(next_path, obj, fields, result)
    else
      result
    end
  end

  # List
  defp data(path, parent, %{values: values} = field, result) do
    IO.inspect({path, values}, label: "list", limit: 12)

    if on_path?(field, path) do
      case path do
        [next, pos | rest] ->
          val = Enum.at(values, pos)

          # result = Result.add_relation_data(result, {Ref.from(parent), field_name(emitter), [obj]})
          IO.inspect({val, path, parent}, label: "LISTVAL", limit: 12)
          data([next | rest], parent, val, result)

        _ ->
          Enum.reduce(values, result, fn val, acc ->
            IO.inspect({val, path, parent}, label: "LISTVAL", limit: 12)
            data(path, parent, val, acc)
          end)
      end

      # Enum.reduce(values, result, &data(path, parent, &1, &2))
    else
      result
    end
  end

  defp field_data(_path, _parent, [], result), do: result

  defp field_data(path, parent, [%Absinthe.Resolution{} = res | fields], result) do
    result =
      if res.value && on_path?(%{emitter: res.definition}, path) do
        IO.inspect({path, Map.delete(res, :acc)},
          label: "RESOLUTION",
          limit: 20
        )
      else
        result
      end

    field_data(path, parent, fields, result)
  end

  defp field_data(path, parent, [field | fields], result) do
    IO.inspect({path, field_name(field.emitter), on_path?(field, path)}, label: "FIELD")

    result =
      if on_path?(field, path) do
        data(path, parent, field, result)
      else
        result
      end

    field_data(path, parent, fields, result)
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
end

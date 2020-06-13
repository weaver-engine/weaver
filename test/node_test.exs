defmodule Weaver.NodeTest do
  use ExUnit.Case

  import Weaver.Node

  describe "id_for/1" do
    test "uses :id field of a Map" do
      assert id_for(%{name: "Carol", id: 12366}) == 12366
    end

    test "uses \"id\" field of a Map" do
      assert id_for(%{"name" => "Carol", "id" => 12366}) == 12366
    end

    test "raises error if not implemented" do
      assert_raise Protocol.UndefinedError,
                   ~r/^protocol Weaver.Node not implemented for \"Howdy!\" of type BitString./,
                   fn -> id_for("Howdy!") end
    end
  end
end

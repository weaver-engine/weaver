defmodule WeaverTest do
  use ExUnit.Case
  doctest Weaver

  test "greets the world" do
    assert Weaver.hello() == :world
  end
end

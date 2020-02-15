defmodule Weaver.GraphTest do
  use Weaver.IntegrationCase, async: false

  doctest Weaver.Graph

  describe "data integrity" do
    import Weaver.Graph, only: [store!: 2, query: 1, cursors: 3]

    setup :use_graph

    setup do
      user1 = %Ref{id: "TwitterUser:elixirdigest"}
      user2 = %Ref{id: "TwitterUser:elixirlang"}

      data = [
        {user1, "screenName", "elixirdigest"},
        {user1, "follows", user2},
        {user2, "screenName", "elixirlang"}
      ]

      meta = [
        {:add, user1, "favorites", %Cursor{val: 140, gap: false, ref: %Ref{id: "Tweet:140"}}},
        {:add, user1, "favorites", %Cursor{val: 134, gap: true, ref: %Ref{id: "Tweet:134"}}}
      ]

      store!(data, meta)

      {:ok, user1: user1}
    end

    test "data" do
      query = ~s"""
      {
        user(func: eq(id, "TwitterUser:elixirdigest")) {
          screenName
        }
      }
      """

      assert {:ok, %{"user" => [%{"screenName" => "elixirdigest"}]}} = query(query)
    end

    test "cursors", %{user1: user1} do
      assert {:ok, [cursor1, cursor2]} = cursors(user1, "favorites", 3)
      assert cursor1 == %Cursor{val: 140, gap: false, ref: %Ref{id: "Tweet:140"}}
      assert cursor2 == %Cursor{val: 134, gap: true, ref: %Ref{id: "Tweet:134"}}
    end

    test "delete cursors", %{user1: user1} do
      assert %{} =
               store!([], [
                 {:del, user1, "favorites",
                  %Cursor{val: 140, gap: false, ref: %Ref{id: "Tweet:140"}}}
               ])

      assert {:ok, [cursor2]} = cursors(user1, "favorites", 3)
      assert cursor2 == %Cursor{val: 134, gap: true, ref: %Ref{id: "Tweet:134"}}
    end
  end
end

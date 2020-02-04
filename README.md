# Weaver

A graph streaming engine written in Elixir

## Installation

Add `weaver` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:weaver, github: "weaver-engine/weaver"}
  ]
end
```

## Loom

While `Weaver.weave` only runs a single step at a time and returns the results,
Loom helps to run all required steps to stream a query. Under the hood it
supervises a topology of `GenStage` workers that you can run as part of your
supervision tree:

```elixir
defmodule MyApp.Application do
  ...

  def start(_type, _args) do
    children = [
      ...

      Weaver.Loom
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

To weave a query, call `Weaver.Loom.weave`.

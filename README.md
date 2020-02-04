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

To use [Loom](#loom), also add the forked version of `GenStage`:

```elixir
def deps do
  [
    {:weaver, github: "weaver-engine/weaver"},
    {:gen_stage, github: "weaver-engine/gen_stage", branch: "prosumer"}
  ]
end
```

The fork is equivalent to `GenStage`, except some checks are removed to
allow for `producer` modules to also subscribe to other Producers (see
[diff](https://github.com/elixir-lang/gen_stage/compare/master...weaver-engine:prosumer)
and [issue comment](https://github.com/elixir-lang/gen_stage/issues/214#issuecomment-432434476)
by José Valim).

## Loom

While `Weaver.weave` only runs a single step at a time and returns the results,
Loom helps to run all required steps and streams the results. Under the hood it
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

To stream a query, call `Weaver.Loom.weave`.

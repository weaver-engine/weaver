defmodule Weaver.GenStage.Consumer do
  @moduledoc """
  Represents a worker at the bottom `GenStage` level.
  Dispatched jobs are handled recursively via `Weaver.Events.handle/1`.
  A job is passed again to `Weaver.Events.handle/1` as long as it
  returns a continuation (`Weaver.Tree` with a `:cursor`).
  Otherwise, it sends demand to the `GenStage` level above.

  Implements a `GenStage` `consumer`.
  """

  use GenStage

  @max_demand 1

  def start_link(opts = {name, _subscriptions}) do
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  @impl GenStage
  def init({name, subscriptions}) do
    subscriptions =
      Enum.map(subscriptions, fn
        {name, opts} -> {name, Keyword.put_new(opts, :max_demand, @max_demand)}
        name -> {name, max_demand: @max_demand}
      end)

    {:consumer, %{name: name}, subscribe_to: subscriptions}
  end

  @impl GenStage
  def handle_events(events, _from, state) do
    {[], nil} = handle_remaining(events, state)
    {:noreply, [], state}
  end

  defp handle_remaining(events, state = %{retrieval: event}) when event != nil do
    {new_events, state} = Weaver.Events.do_handle(event, state)
    handle_remaining(new_events ++ events, state)
  end

  defp handle_remaining([event | events], state) do
    {new_events, state} = Weaver.Events.do_handle(event, state)
    handle_remaining(new_events ++ events, state)
  end

  defp handle_remaining([], state) do
    {[], state}
  end
end

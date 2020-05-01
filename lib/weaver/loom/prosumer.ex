defmodule Weaver.Loom.Prosumer do
  @moduledoc """
  Represents a worker that handles one `Weaver.Step` at a time.
  Dispatched steps are passed to the `GenStage` level below after each call
  to `Weaver.Step.process/1`. A step is passed again to `Weaver.Step.process/1`
  as long as it returns a new `Weaver.Step` as `next`.
  Otherwise, it sends demand to the `GenStage` level above.

  Implements a `GenStage` `producer` that is also a `consumer` with manual
  demand handling via `GenStage.ask/2` (see `handle_subscribe/4` and `handle_info/2`).
  """

  use GenStage

  @max_demand 1

  alias __MODULE__.State

  defmodule State do
    @moduledoc false

    defstruct [:name, :status, :retrieval, producers: %{}, demand: 0, queue: []]
  end

  def start_link(opts = {name, _subscriptions}) do
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  @impl GenStage
  def init({name, subscriptions}) do
    Enum.each(subscriptions, fn subscription ->
      opts =
        case subscription do
          {name, opts} -> [{:to, name} | opts]
          name -> [to: name]
        end
        |> Keyword.put_new(:max_demand, @max_demand)

      GenStage.async_subscribe(self(), opts)
    end)

    {:producer, %State{name: name, status: :waiting_for_consumers}}
  end

  @impl GenStage
  def handle_subscribe(:producer, opts, from, state) do
    pending = opts[:max_demand] || @max_demand

    state = put_in(state.producers[from], pending)
    if state.status == :waiting_for_producers, do: GenStage.ask(from, pending)

    # Returns manual as we want control over the demand
    {:manual, state}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  @impl GenStage
  def handle_cancel(_, from, state) do
    # Remove the producers from the map on unsubscribe
    producers = Map.delete(state.producers, from)

    {:noreply, [], %{state | producers: producers}}
  end

  @impl GenStage
  def handle_events(events, from, state) when is_list(events) do
    state =
      update_in(state.producers[from], &(&1 + length(events)))
      |> Map.update!(:queue, &(&1 ++ events))

    noreply([], state)
  end

  @impl GenStage
  def handle_demand(demand, state) do
    noreply([], state, demand)
  end

  @impl GenStage
  def handle_info(:tick, state = %{demand: 0}) do
    {:noreply, [], %{state | status: :waiting_for_consumers}}
  end

  def handle_info(:tick, state = %{retrieval: event}) when event != nil do
    state = %{state | status: :working}

    case Weaver.Loom.Event.process(event) do
      {:ok, dispatched, next} ->
        noreply(dispatched, %{state | retrieval: next})

      {:retry, event, delay} ->
        Process.send_after(self(), :tick, delay)
        {:noreply, [], %{state | retrieval: event, status: :paused}}

      {:error, _} ->
        noreply([], %{state | retrieval: nil})
    end
  end

  def handle_info(:tick, state = %{queue: [event | queue]}) do
    noreply([], %{state | retrieval: event, queue: queue})
  end

  def handle_info(:tick, state) do
    producers =
      Enum.into(state.producers, %{}, fn {from, pending} ->
        # Ask for any pending events
        GenStage.ask(from, pending)

        # Reset pending events to 0
        {from, 0}
      end)

    {:noreply, [], %{state | producers: producers, status: :waiting_for_producers}}
  end

  defp noreply(events, state, demand \\ 0) do
    count = length(events)
    new_demand = max(state.demand + demand - count, 0)
    state = %{state | demand: new_demand}
    if new_demand > 0, do: send(self(), :tick)

    {:noreply, events, state}
  end
end

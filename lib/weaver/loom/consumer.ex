defmodule Weaver.Loom.Consumer do
  @moduledoc """
  Represents a worker at the bottom `GenStage` level.
  Dispatched steps are processed recursively via `Weaver.Step.process/1`.
  A step is passed again to `Weaver.Step.process/1` as long as it
  returns a new `Weaver.Step` as `next`.
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
    :ok = handle_remaining(events)
    {:noreply, [], state}
  end

  defp handle_remaining([event | events]) do
    try do
      result = Weaver.Step.process(event.step)

      case event.callback.(result, event.assigns) do
        {:ok, {_data, _meta, dispatched, next}, assigns} ->
          dispatched =
            Enum.map(dispatched, fn step ->
              %{event | step: step, assigns: assigns}
            end)

          next = if next, do: %{event | step: next, assigns: assigns}

          handle_remaining(dispatched ++ List.wrap(next) ++ events)

        {:error, _} ->
          handle_remaining(events)
      end
    rescue
      e in ExTwitter.RateLimitExceededError ->
        Process.sleep(:timer.seconds(e.reset_in))
        handle_remaining([event | events])

      _e in Dlex.Error ->
        Process.sleep(:timer.seconds(5))
        handle_remaining([event | events])

      e ->
        case event.callback.({:error, e}, event.assigns) do
          {:retry, assigns} ->
            retry_event = %{event | assigns: assigns}

            handle_remaining([retry_event | events])

          :ok ->
            handle_remaining(events)
        end
    end
  end

  defp handle_remaining([]) do
    :ok
  end
end

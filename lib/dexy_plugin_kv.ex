defmodule DexyPluginKV do

  @app :dexy_plugin_kv

  @adapter Application.get_env(@app, __MODULE__)[:adapter] || __MODULE__.Adapters.Riak
  @bucket Application.get_env(@app, __MODULE__)[:bucket]

  defmodule Adapter do
    @type error :: {:error, reason}
    @type reason :: term
    @type result :: {:ok, term} | error
    @type bucket :: binary
    @type key :: binary
    @type value :: any

    @callback get(bucket, key) :: result
    @callback put(bucket, key, value, Keywords.t) :: result
    @callback delete(bucket, key) :: :ok | error
  end

  use DexyLib, as: Lib
  deferror Error.InvalidArgument
  deferror Error.BucketLengthExceeded
  deferror Error.KeyLengthExceeded

  @max_bucket_bytes \
    Application.get_env(:dexy_plugin_kv, __MODULE__)[:max_bucket_bytes] || 128

  @max_key_bytes \
    Application.get_env(:dexy_plugin_kv, __MODULE__)[:max_key_bytes] || 128

  def get state = %{args: [], opts: opts} do
    do_get state, bucket_key(opts, state)
  end

  defp do_get state, {bucket, key} do
    res = case @adapter.get(bucket, key) do
      {:ok, val} -> val
      {:error, _} -> nil
    end
    {state, res}
  end

  def put state = %{args: [], opts: opts} do
    {bucket, key} = bucket_key(opts, state)
    do_put state, {bucket, key, data! state}
  end

  def put state = %{args: [value], opts: opts} do
    {bucket, key} = bucket_key(opts, state)
    do_put state, {bucket, key, value}
  end

  defp do_put state, {bucket, key, val} do
    res = case @adapter.put(bucket, key, val) do
      :ok -> "ok"
      {:error, _} -> nil
    end
    {state, res}
  end

  def new state = %{opts: opts} do
    with \
      {bucket, key} = bucket_key(opts, state),
      {:error, _} <- @adapter.get(bucket, key),
      :ok <- put state
    do
      {state, "ok"} else _ -> {state, nil}
    end
  end

  def delete state = %{args: [], opts: opts} do
    do_delete state, bucket_key(opts, state)
  end

  defp do_delete state, {bucket, key} do
    {state, @adapter.delete(bucket, key)}
  end

  defp bucket_key map, state do
    bkt = map["bucket"] || ""
    key = map["key"] || ""
    is_bitstring(bkt) && is_bitstring(key) || raise Error.InvalidArgument, state: state
    (byte_size(bkt) > @max_bucket_bytes) && raise Error.BucketLengthExceeded
    (byte_size(key) > @max_key_bytes) && raise Error.KeyLengthExceeded
    {@bucket, state.req.user <> ":" <> bkt <> ":" <> key}
  end

  defp data! %{mappy: map} do
    Lib.Mappy.val map, "data", nil
  end

end

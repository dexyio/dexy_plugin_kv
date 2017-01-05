defmodule DexyPluginKV do

  @app :dexy_plugin_kv

  @adapter Application.get_env(@app, __MODULE__)[:adapter] || __MODULE__.Adapters.Riak

  defmodule Adapter do
    @type error :: {:error, reason}
    @type reason :: term
    @type result :: {:ok, term} | error
    @type user :: bitstring
    @type bucket :: bitstring
    @type key :: bitstring
    @type value :: any
    @type index :: bitstring
    @type query :: bitstring
    @type opts :: Keyword.t

    @callback put(user, bucket, key, value, Keywords.t) :: result
    @callback get(user, bucket, key) :: result

    #@callback create(bucket, key) :: :ok | error
    @callback delete(user, bucket, key) :: :ok | error

    @callback get_all(user, bucket) :: result
    @callback delete_all(user, bucket) :: :ok | error

    @callback buckets(user) :: result 
    @callback keys(user, bucket) :: result

    @callback search(query, opts) :: result
  end

  use DexyLib, as: Lib
  require Logger

  deferror Error.InvalidArgument
  deferror Error.BucketLengthExceeded
  deferror Error.KeyLengthExceeded
  deferror Error.KeyDelimiterNotConfigured

  @max_bucket_bytes Application.get_env(@app, __MODULE__)[:max_bucket_bytes]
    || (Logger.warn "max_bucket_bytes not configured, default: 128"; 128)

  @max_key_bytes Application.get_env(@app, __MODULE__)[:max_key_bytes]
    || (Logger.warn "max_key_bytes not configured, default: 128"; 128)

  def get state = %{args: [], opts: opts} do
    do_get state, bucket_key(opts, state)
  end

  defp do_get state = %{user: user}, {bucket, key} do
    res = case @adapter.get(user.id, bucket, key) do
      {:ok, val} -> val
      {:error, _} -> nil
    end
    {state, res}
  end

  def get_all state = %{args: [], opts: opts} do
    {bucket, _} = bucket_key(opts, state)
    do_get_all state, bucket
  end

  defp do_get_all state = %{user: user}, bucket do
    res = case @adapter.get_all(user.id, bucket) do
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

  defp do_put state = %{user: user}, {bucket, key, val} do
    res = case @adapter.put(user.id, bucket, key, val) do
      :ok -> "ok"
      {:error, _} -> nil
    end
    {state, res}
  end

  def create state = %{user: user, opts: opts} do
    with \
      {bucket, key} = bucket_key(opts, state),
      {:error, _} <- @adapter.get(user.id, bucket, key),
      :ok <- put state
    do
      {state, "ok"} else _ -> {state, nil}
    end
  end

  def delete state = %{args: [], opts: opts} do
    do_delete state, bucket_key(opts, state)
  end

  defp do_delete state = %{user: user}, {bucket, key} do
    {state, @adapter.delete(user.id, bucket, key)}
  end

  def buckets state = %{args: []} do
    do_buckets state
  end

  defp do_buckets state = %{user: user} do
    {state, @adapter.buckets(user.id)}
  end

  defp bucket_key map, state do
    bucket = map["bucket"] || ""
    key = map["key"] || ""
    check_bucket_key! bucket, key, state
    {bucket, key}
  end

  defp check_bucket_key! bucket, key, state do
    is_bitstring(bucket) && is_bitstring(key)
      || (raise Error.InvalidArgument, state: state)
    (byte_size(bucket) > @max_bucket_bytes) && raise Error.BucketLengthExceeded
    (byte_size(key) > @max_key_bytes) && raise Error.KeyLengthExceeded
  end

  defp data! %{mappy: map} do
    Lib.Mappy.val map, "data", nil
  end

end

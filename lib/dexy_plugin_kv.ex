defmodule DexyPluginKV do

  require Logger

  @app :dexy_plugin_kv

  @adapter Application.get_env(@app, __MODULE__)[:adapter] || __MODULE__.Adapters.Riak

  @default_search_rows 1000
  @search_rows Application.get_env(@app, __MODULE__)[:search_rows] || (
    Logger.warn "search_rows: not configured, default: #{@default_search_rows}";
    @default_search_rows
  )

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
    @type query_or_opts :: query | opts
    @type search_opts :: opts

    @callback put(user, bucket, key, value, opts) :: result
    @callback get(user, bucket, key) :: result

    #@callback create(bucket, key) :: :ok | error
    @callback delete(user, bucket, key) :: :ok | error

    @callback delete_all(user, bucket) :: :ok | error

    @callback buckets(user) :: result 
    @callback keys(user, bucket) :: result

    @callback search(user, query_or_opts, search_opts) :: result
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

  def put state = %{args: [], opts: opts} do
    {bucket, key} = bucket_key(opts, state)
    do_put state, {bucket, key, data! state}
  end

  def put state = %{args: [value], opts: opts} do
    {bucket, key} = bucket_key(opts, state)
    do_put state, {bucket, key, value}
  end

  defp do_put state = %{user: user, opts: opts}, {bucket, key, val} do
    res = case @adapter.put(user.id, bucket, key, val, opts) do
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

  def search state = %{args: []} do do_search state, nil end
  def search state = %{args: [query]} do do_search state, query end

  defp do_search state = %{user: user, opts: opts}, query do
    search_opts = do_search_opts opts
    @adapter.search(user.id, query || opts, search_opts)
      |> case do
        {:ok, res} -> {state, res}
        {:error, _reason} -> {state, []}
      end
  end

  defp do_search_opts opts do
    opts2 = [start: opts["start"] || 0, rows: opts["rows"] || @search_rows]
    opts2 = (sort = opts["sort"]) && [{:sort, sort} | opts2] || opts2
    (timeout = opts["timeout"]) && [{:timeout, timeout} | opts2] || opts2
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
    Lib.Mappy.val map, "data"
  end

end

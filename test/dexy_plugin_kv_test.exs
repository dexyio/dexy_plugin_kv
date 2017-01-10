defmodule DexyPluginKVTest do

  use ExUnit.Case
  doctest DexyPluginKV

  alias DexyPluginKV, as: KV

  Application.start :pooler
  @user %{id: "test2"}

  test "simple put & get" do
    opts = %{}
    KV.put %{user: @user, args: [1], opts: opts}
    assert {_, 1} = KV.get %{user: @user, args: [], opts: opts}

    opts = %{"bucket"=>"foo"}
    KV.put %{user: @user, args: [2], opts: opts}
    assert {_, 2} = KV.get %{user: @user, args: [], opts: opts}

    opts = %{"bucket"=>"foo", "key"=>"bar"}
    KV.put %{user: @user, args: ["3"], opts: opts}
    assert {_, "3"} = KV.get %{user: @user, args: [], opts: opts}
  end

  test "buckets" do
    %{"bucket" => "foo"}
    |> get_all |> IO.inspect
  end

  test "simple search" do
    res1 = %{} |> search |> IO.inspect 
    res2 = %{"query" => ""} |> search |> IO.inspect 
    res3 = %{"query" => "*:*"} |> search |> IO.inspect 
    assert res1 = res2 = res3

    res1 = %{"bucket" => "foo", "key" => "bar*"} |> search |> IO.inspect 
    res2 = %{"query" => "bucket:foo AND key:bar*"} |> search |> IO.inspect 
    assert res1 == res2
  end

  test "complex put & get" do
    opts = %{"bucket" => "company1.foo", "key" => "001"}
      |> Map.merge(%{"tags" => ["company1", "foo"]})
    assert "ok" == put "hi", opts
    assert "hi" == get opts
  end

  test "complex search" do
    res1 = %{"bucket" => "company1.*"} |> search |> IO.inspect 
    res2 = %{"query" => "bucket:company1.*"} |> search |> IO.inspect 
    res3 = %{"query" => "tags:company1"} |> search |> IO.inspect 
    assert res1 = res2 = res3
  end

  defp put value, opts do
    {_state, res} = KV.put %{mappy: %{}, user: @user, args: [value], opts: opts}
    res
  end

  defp get opts do
    {_state, val} = KV.get %{mappy: %{}, user: @user, args: [], opts: opts}
    val
  end

  defp get_all opts do
    {_state, val} = KV.get_all %{mappy: %{}, user: @user, args: [], opts: opts}
    val
  end

  defp search opts do
    query = opts["query"]
    {_state, res} = KV.search %{mappy: %{}, user: @user, args: [query], opts: opts}
    res
  end

end

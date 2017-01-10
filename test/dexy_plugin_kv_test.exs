defmodule DexyPluginKVTest do

  use ExUnit.Case
  doctest DexyPluginKV

  alias DexyPluginKV, as: KV

  Application.start :pooler
  @user %{id: "test2"}

  test "put & get" do
    opts = %{}

    KV.delete %{user: @user, args: [], opts: opts}
    assert {_, nil} = KV.get%{user: @user, args: [], opts: opts}

    KV.put %{user: @user, args: ["hi"], opts: opts}
    assert {_, "hi"} = KV.get %{user: @user, args: [], opts: opts}

    opts = %{"bucket"=>"foo"}

    KV.delete %{user: @user, args: [], opts: opts}
    assert {_, nil} = KV.get%{user: @user, args: [], opts: opts}

    KV.put %{user: @user, args: ["hi"], opts: opts}
    assert {_, "hi"} = KV.get %{user: @user, args: [], opts: opts}

    opts = %{"bucket"=>"foo", "key"=>"bar"}

    KV.delete %{user: @user, args: [], opts: opts}
    assert {_, nil} = KV.get%{user: @user, args: [], opts: opts}

    KV.put %{user: @user, args: ["hi"], opts: opts}
    assert {_, "hi"} = KV.get %{user: @user, args: [], opts: opts}
  end

  test "buckets" do
    opts = %{"bucket" => "foo"}
    {_, res} = KV.get_all %{user: @user, args: [], opts: opts}
    IO.inspect res
  end

  test "search" do
    opts = %{}
    {_, res} = KV.search %{user: @user, args: [], opts: opts}
    IO.inspect res
  end

end

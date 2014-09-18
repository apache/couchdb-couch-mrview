% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_mrview_http).

-export([
    handle_all_docs_req/2,
    handle_view_req/3,
    handle_temp_view_req/2,
    handle_info_req/3,
    handle_compact_req/3,
    handle_cleanup_req/2
]).

-export([
    parse_boolean/1,
    parse_int/1,
    parse_pos_int/1,
    prepend_val/1,
    parse_params/2,
    parse_params/3,
    view_cb/2,
    row_to_json/1,
    row_to_json/2,
    check_view_etag/3
]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


handle_all_docs_req(#httpd{method='GET'}=Req, Db) ->
    all_docs_req(Req, Db, undefined);
handle_all_docs_req(#httpd{method='POST'}=Req, Db) ->
    Keys = couch_mrview_util:get_view_keys(couch_httpd:json_body_obj(Req)),
    all_docs_req(Req, Db, Keys);
handle_all_docs_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST,HEAD").


handle_view_req(#httpd{method='GET'}=Req, Db, DDoc) ->
    [_, _, _, _, ViewName] = Req#httpd.path_parts,
    couch_stats:increment_counter([couchdb, httpd, view_reads]),
    design_doc_view(Req, Db, DDoc, ViewName, undefined);
handle_view_req(#httpd{method='POST'}=Req, Db, DDoc) ->
    [_, _, _, _, ViewName] = Req#httpd.path_parts,
    Props = couch_httpd:json_body_obj(Req),
    Keys = couch_mrview_util:get_view_keys(Props),
    Queries = couch_mrview_util:get_view_queries(Props),
    case {Queries, Keys} of
        {Queries, undefined} when is_list(Queries) ->
            IncrBy = length(Queries),
            couch_stats:increment_counter([couchdb, httpd, view_reads], IncrBy),
            multi_query_view(Req, Db, DDoc, ViewName, Queries);
        {undefined, Keys} when is_list(Keys) ->
            couch_stats:increment_counter([couchdb, httpd, view_reads]),
            design_doc_view(Req, Db, DDoc, ViewName, Keys);
        {undefined, undefined} ->
            throw({
                bad_request,
                "POST body must contain `keys` or `queries` field"
            });
        {_, _} ->
            throw({bad_request, "`keys` and `queries` are mutually exclusive"})
    end;
handle_view_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST,HEAD").


handle_temp_view_req(#httpd{method='POST'}=Req, Db) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = couch_db:check_is_admin(Db),
    {Body} = couch_httpd:json_body_obj(Req),
    DDoc = couch_mrview_util:temp_view_to_ddoc({Body}),
    Keys = couch_mrview_util:get_view_keys({Body}),
    couch_stats:increment_counter([couchdb, httpd, temporary_view_reads]),
    design_doc_view(Req, Db, DDoc, <<"temp">>, Keys);
handle_temp_view_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "POST").


handle_info_req(#httpd{method='GET'}=Req, Db, DDoc) ->
    [_, _, Name, _] = Req#httpd.path_parts,
    {ok, Info} = couch_mrview:get_info(Db, DDoc),
    couch_httpd:send_json(Req, 200, {[
        {name, Name},
        {view_index, {Info}}
    ]});
handle_info_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET").


handle_compact_req(#httpd{method='POST'}=Req, Db, DDoc) ->
    ok = couch_db:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = couch_mrview:compact(Db, DDoc),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});
handle_compact_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "POST").


handle_cleanup_req(#httpd{method='POST'}=Req, Db) ->
    ok = couch_db:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = couch_mrview:cleanup(Db),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});
handle_cleanup_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "POST").


all_docs_req(Req, Db, Keys) ->
    case couch_db:is_system_db(Db) of
    true ->
        case (catch couch_db:check_is_admin(Db)) of
        ok ->
            do_all_docs_req(Req, Db, Keys);
        _ ->
            DbName = ?b2l(Db#db.name),
            case config:get("couch_httpd_auth",
                                  "authentication_db",
                                  "_users") of
            DbName ->
                UsersDbPublic = config:get("couch_httpd_auth", "users_db_public", "false"),
                PublicFields = config:get("couch_httpd_auth", "public_fields"),
                case {UsersDbPublic, PublicFields} of
                {"true", PublicFields} when PublicFields =/= undefined ->
                    do_all_docs_req(Req, Db, Keys);
                {_, _} ->
                    throw({forbidden, <<"Only admins can access _all_docs",
                                        " of system databases.">>})
                end;
            _ ->
                throw({forbidden, <<"Only admins can access _all_docs",
                                    " of system databases.">>})
            end
        end;
    false ->
        do_all_docs_req(Req, Db, Keys)
    end.

do_all_docs_req(Req, Db, Keys) ->
    Args0 = parse_params(Req, Keys),
    ETagFun = fun(Sig, Acc0) ->
        check_view_etag(Sig, Acc0, Req)
    end,
    Args = Args0#mrargs{preflight_fun=ETagFun},
    {ok, Resp} = couch_httpd:etag_maybe(Req, fun() ->
        VAcc0 = #vacc{db=Db, req=Req},
        DbName = ?b2l(Db#db.name),
        UsersDbName = config:get("couch_httpd_auth",
                                 "authentication_db",
                                 "_users"),
        IsAdmin = is_admin(Db),
        Callback = get_view_callback(DbName, UsersDbName, IsAdmin),
        couch_mrview:query_all_docs(Db, Args, Callback, VAcc0)
    end),
    case is_record(Resp, vacc) of
        true -> {ok, Resp#vacc.resp};
        _ -> {ok, Resp}
    end.

is_admin(Db) ->
    case catch couch_db:check_is_admin(Db) of
    {unauthorized, _} ->
        false;
    ok ->
        true
    end.


% admin users always get all fields
get_view_callback(_, _, true) ->
    fun view_cb/2;
% if we are operating on the users db and we aren't
% admin, filter the view
get_view_callback(_DbName, _DbName, false) ->
    fun filtered_view_cb/2;
% non _users databases get all fields
get_view_callback(_, _, _) ->
    fun view_cb/2.


design_doc_view(Req, Db, DDoc, ViewName, Keys) ->
    Args0 = parse_params(Req, Keys),
    ETagFun = fun(Sig, Acc0) ->
        check_view_etag(Sig, Acc0, Req)
    end,
    Args = Args0#mrargs{preflight_fun=ETagFun},
    {ok, Resp} = couch_httpd:etag_maybe(Req, fun() ->
        VAcc0 = #vacc{db=Db, req=Req},
        couch_mrview:query_view(Db, DDoc, ViewName, Args, fun view_cb/2, VAcc0)
    end),
    case is_record(Resp, vacc) of
        true -> {ok, Resp#vacc.resp};
        _ -> {ok, Resp}
    end.


multi_query_view(Req, Db, DDoc, ViewName, Queries) ->
    Args0 = parse_params(Req, undefined),
    {ok, _, _, Args1} = couch_mrview_util:get_view(Db, DDoc, ViewName, Args0),
    ArgQueries = lists:map(fun({Query}) ->
        QueryArg = parse_params(Query, undefined, Args1),
        couch_mrview_util:validate_args(QueryArg)
    end, Queries),
    {ok, Resp2} = couch_httpd:etag_maybe(Req, fun() ->
        VAcc0 = #vacc{db=Db, req=Req, prepend="\r\n"},
        %% TODO: proper calculation of etag
        Etag = couch_uuids:new(),
        Headers = [{"ETag", Etag}],
        FirstChunk = "{\"results\":[",
        {ok, Resp0} = chttpd:start_delayed_json_response(VAcc0#vacc.req, 200, Headers, FirstChunk),
        VAcc1 = VAcc0#vacc{resp=Resp0},
        VAcc2 = lists:foldl(fun(Args, Acc0) ->
            {ok, Acc1} = couch_mrview:query_view(Db, DDoc, ViewName, Args, fun view_cb/2, Acc0),
            Acc1
        end, VAcc1, ArgQueries),
        {ok, Resp1} = chttpd:send_delayed_chunk(VAcc2#vacc.resp, "\r\n]}"),
        {ok, Resp2} = chttpd:end_delayed_json_response(Resp1),
        {ok, VAcc2#vacc{resp=Resp2}}
    end),
    case is_record(Resp2, vacc) of
        true -> {ok, Resp2#vacc.resp};
        _ -> {ok, Resp2}
    end.


filtered_view_cb({row, Row0}, Acc) ->
  Row1 = lists:map(fun({doc, null}) ->
        {doc, null};
    ({doc, Body}) ->
        Doc = couch_users_db:strip_non_public_fields(#doc{body=Body}),
        {doc, Doc#doc.body};
    (KV) ->
        KV
    end, Row0),
    view_cb({row, Row1}, Acc);
filtered_view_cb(Obj, Acc) ->
    view_cb(Obj, Acc).


view_cb({meta, Meta}, #vacc{resp=undefined}=Acc) ->
    % Map function starting
    Headers = [{"ETag", Acc#vacc.etag}],
    {ok, Resp} = chttpd:start_delayed_json_response(Acc#vacc.req, 200, Headers),
    view_cb({meta, Meta}, Acc#vacc{resp=Resp, should_close=true});
view_cb({meta, Meta}, #vacc{resp=Resp}=Acc) ->
    % Sending metadata
    Parts = case couch_util:get_value(total, Meta) of
        undefined -> [];
        Total -> [io_lib:format("\"total_rows\":~p", [Total])]
    end ++ case couch_util:get_value(offset, Meta) of
        undefined -> [];
        Offset -> [io_lib:format("\"offset\":~p", [Offset])]
    end ++ case couch_util:get_value(update_seq, Meta) of
        undefined -> [];
        UpdateSeq -> [io_lib:format("\"update_seq\":~p", [UpdateSeq])]
    end ++ ["\"rows\":["],
    Prepend = prepend_val(Acc),
    Chunk = lists:flatten(Prepend ++ "{" ++ string:join(Parts, ",") ++ "\r\n"),
    {ok, Resp1} = chttpd:send_delayed_chunk(Resp, Chunk),
    {ok, Acc#vacc{resp=Resp1, prepend=""}};
view_cb({row, Row}, Acc) ->
    % Adding another row
    Chunk = [prepend_val(Acc), row_to_json(Row)],
    {ok, Resp1} = chttpd:send_delayed_chunk(Acc#vacc.resp, Chunk),
    {ok, Acc#vacc{prepend=",\r\n", resp=Resp1}};
view_cb(complete, #vacc{resp=undefined}=Acc) ->
    % Nothing in view
    {ok, Resp} = chttpd:send_json(Acc#vacc.req, 200, {[{rows, []}]}),
    {ok, Acc#vacc{resp=Resp}};
view_cb(complete, #vacc{resp=Resp}=Acc) ->
    % Finish view output and possibly end the response
    {ok, Resp1} = chttpd:send_delayed_chunk(Resp, "\r\n]}"),
    case Acc#vacc.should_close of
        true ->
            {ok, Resp2} = chttpd:end_delayed_json_response(Resp1),
            {ok, Acc#vacc{resp=Resp2}};
        _ ->
            {ok, Acc#vacc{resp=Resp1, prepend=",\r\n"}}
    end;
view_cb({error, Reason}, #vacc{resp=undefined}=Acc) ->
    {ok, Resp} = chttpd:send_error(Acc#vacc.req, Reason),
    {ok, Acc#vacc{resp=Resp}};
view_cb({error, Reason}, #vacc{resp=Resp}=Acc) ->
    {ok, Resp1} = chttpd:send_delayed_error(Resp, Reason),
    {ok, Acc#vacc{resp=Resp1}}.


prepend_val(#vacc{prepend=Prepend}) ->
    case Prepend of
        undefined ->
            "";
        _ ->
            Prepend
    end.


row_to_json(Row) ->
    Id = couch_util:get_value(id, Row),
    row_to_json(Id, Row).


row_to_json(error, Row) ->
    % Special case for _all_docs request with KEYS to
    % match prior behavior.
    Key = couch_util:get_value(key, Row),
    Val = couch_util:get_value(value, Row),
    Obj = {[{key, Key}, {error, Val}]},
    ?JSON_ENCODE(Obj);
row_to_json(Id0, Row) ->
    Id = case Id0 of
        undefined -> [];
        Id0 -> [{id, Id0}]
    end,
    Key = couch_util:get_value(key, Row, null),
    Val = couch_util:get_value(value, Row),
    Doc = case couch_util:get_value(doc, Row) of
        undefined -> [];
        Doc0 -> [{doc, Doc0}]
    end,
    Obj = {Id ++ [{key, Key}, {value, Val}] ++ Doc},
    ?JSON_ENCODE(Obj).


parse_params(#httpd{}=Req, Keys) ->
    parse_params(couch_httpd:qs(Req), Keys);
parse_params(Props, Keys) ->
    Args = #mrargs{},
    parse_params(Props, Keys, Args).


parse_params(Props, Keys, #mrargs{}=Args0) ->
    Args = Args0#mrargs{keys=Keys},
    lists:foldl(fun({K, V}, Acc) ->
        parse_param(K, V, Acc)
    end, Args, Props).


parse_param(Key, Val, Args) when is_binary(Key) ->
    parse_param(binary_to_list(Key), Val, Args);
parse_param(Key, Val, Args) ->
    case Key of
        "" ->
            Args;
        "reduce" ->
            Args#mrargs{reduce=parse_boolean(Val)};
        "key" ->
            JsonKey = parse_json(Val),
            Args#mrargs{start_key=JsonKey, end_key=JsonKey};
        "keys" ->
            Args#mrargs{keys=parse_json(Val)};
        "startkey" ->
            Args#mrargs{start_key=parse_json(Val)};
        "start_key" ->
            Args#mrargs{start_key=parse_json(Val)};
        "startkey_docid" ->
            Args#mrargs{start_key_docid=couch_util:to_binary(Val)};
        "start_key_doc_id" ->
            Args#mrargs{start_key_docid=couch_util:to_binary(Val)};
        "endkey" ->
            Args#mrargs{end_key=parse_json(Val)};
        "end_key" ->
            Args#mrargs{end_key=parse_json(Val)};
        "endkey_docid" ->
            Args#mrargs{end_key_docid=couch_util:to_binary(Val)};
        "end_key_doc_id" ->
            Args#mrargs{end_key_docid=couch_util:to_binary(Val)};
        "limit" ->
            Args#mrargs{limit=parse_pos_int(Val)};
        "count" ->
            throw({query_parse_error, <<"QS param `count` is not `limit`">>});
        "stale" when Val == "ok" orelse Val == <<"ok">> ->
            Args#mrargs{stale=ok};
        "stale" when Val == "update_after" orelse Val == <<"update_after">> ->
            Args#mrargs{stale=update_after};
        "stale" ->
            throw({query_parse_error, <<"Invalid value for `stale`.">>});
        "descending" ->
            case parse_boolean(Val) of
                true -> Args#mrargs{direction=rev};
                _ -> Args#mrargs{direction=fwd}
            end;
        "skip" ->
            Args#mrargs{skip=parse_pos_int(Val)};
        "group" ->
            case parse_boolean(Val) of
                true -> Args#mrargs{group_level=exact};
                _ -> Args#mrargs{group_level=0}
            end;
        "group_level" ->
            Args#mrargs{group_level=parse_pos_int(Val)};
        "inclusive_end" ->
            Args#mrargs{inclusive_end=parse_boolean(Val)};
        "include_docs" ->
            Args#mrargs{include_docs=parse_boolean(Val)};
        "attachments" ->
            case parse_boolean(Val) of
            true ->
                Opts = Args#mrargs.doc_options,
                Args#mrargs{doc_options=[attachments|Opts]};
            false ->
                Args
            end;
        "att_encoding_info" ->
            case parse_boolean(Val) of
            true ->
                Opts = Args#mrargs.doc_options,
                Args#mrargs{doc_options=[att_encoding_info|Opts]};
            false ->
                Args
            end;
        "update_seq" ->
            Args#mrargs{update_seq=parse_boolean(Val)};
        "conflicts" ->
            Args#mrargs{conflicts=parse_boolean(Val)};
        "list" ->
            Args#mrargs{list=couch_util:to_binary(Val)};
        "callback" ->
            Args#mrargs{callback=couch_util:to_binary(Val)};
        _ ->
            BKey = couch_util:to_binary(Key),
            BVal = couch_util:to_binary(Val),
            Args#mrargs{extra=[{BKey, BVal} | Args#mrargs.extra]}
    end.


parse_boolean(true) ->
    true;
parse_boolean(false) ->
    false;
parse_boolean(Val) when is_binary(Val) ->
    parse_boolean(?b2l(Val));
parse_boolean(Val) ->
    case string:to_lower(Val) of
    "true" -> true;
    "false" -> false;
    _ ->
        Msg = io_lib:format("Invalid boolean parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.


parse_int(Val) when is_integer(Val) ->
    Val;
parse_int(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.


parse_pos_int(Val) ->
    case parse_int(Val) of
    IntVal when IntVal >= 0 ->
        IntVal;
    _ ->
        Fmt = "Invalid value for positive integer: ~p",
        Msg = io_lib:format(Fmt, [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.


check_view_etag(Sig, Acc0, Req) ->
    ETag = couch_httpd:make_etag(Sig),
    case couch_httpd:etag_match(Req, ETag) of
        true -> throw({etag_match, ETag});
        false -> {ok, Acc0#vacc{etag=ETag}}
    end.


parse_json(V) when is_list(V) ->
    ?JSON_DECODE(V);
parse_json(V) ->
    V.

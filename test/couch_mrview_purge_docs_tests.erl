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

-module(couch_mrview_purge_docs_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").

-define(TIMEOUT, 1000).


setup() ->
    {ok, Db} = couch_mrview_test_util:init_db(?tempdb(), map, 5),
    Db.

teardown(Db) ->
    couch_db:close(Db),
    couch_server:delete(Db#db.name, [?ADMIN_CTX]),
    ok.

view_purge_test_() ->
    {
        "Map views",
        {
            setup,
            fun test_util:start_couch/0, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun test_purge_single/1,
                    fun test_purge_multiple/1
                ]
            }
        }
    }.


test_purge_single(Db) ->
    ?_test(begin
        Result = run_query(Db, []),
        Expect = {ok, [
            {meta, [{total, 5}, {offset, 0}]},
            {row, [{id, <<"1">>}, {key, 1}, {value, 1}]},
            {row, [{id, <<"2">>}, {key, 2}, {value, 2}]},
            {row, [{id, <<"3">>}, {key, 3}, {value, 3}]},
            {row, [{id, <<"4">>}, {key, 4}, {value, 4}]},
            {row, [{id, <<"5">>}, {key, 5}, {value, 5}]}
        ]},
        ?assertEqual(Expect, Result),

        FDI = couch_db:get_full_doc_info(Db, <<"1">>),
        Rev = get_rev(FDI),
        {ok, {_, _}} = couch_db:purge_docs(Db, [{<<"UUID1">>, <<"1">>, [Rev]}]),
        {ok, Db2} = couch_db:reopen(Db),

        Result2 = run_query(Db2, []),
        Expect2 = {ok, [
            {meta, [{total, 4}, {offset, 0}]},
            {row, [{id, <<"2">>}, {key, 2}, {value, 2}]},
            {row, [{id, <<"3">>}, {key, 3}, {value, 3}]},
            {row, [{id, <<"4">>}, {key, 4}, {value, 4}]},
            {row, [{id, <<"5">>}, {key, 5}, {value, 5}]}
        ]},
        ?assertEqual(Expect2, Result2),

        ok
    end).


test_purge_multiple(Db) ->
    ?_test(begin
        Result = run_query(Db, []),
        Expect = {ok, [
            {meta, [{total, 5}, {offset, 0}]},
            {row, [{id, <<"1">>}, {key, 1}, {value, 1}]},
            {row, [{id, <<"2">>}, {key, 2}, {value, 2}]},
            {row, [{id, <<"3">>}, {key, 3}, {value, 3}]},
            {row, [{id, <<"4">>}, {key, 4}, {value, 4}]},
            {row, [{id, <<"5">>}, {key, 5}, {value, 5}]}
        ]},
        ?assertEqual(Expect, Result),

        FDI1 = couch_db:get_full_doc_info(Db, <<"1">>), Rev1 = get_rev(FDI1),
        FDI2 = couch_db:get_full_doc_info(Db, <<"2">>), Rev2 = get_rev(FDI2),
        FDI5 = couch_db:get_full_doc_info(Db, <<"5">>), Rev5 = get_rev(FDI5),

        IdsRevs = [
            {<<"UUID1">>, <<"1">>, [Rev1]},
            {<<"UUID2">>, <<"2">>, [Rev2]},
            {<<"UUID5">>, <<"5">>, [Rev5]}
        ],
        {ok, {_, _}} = couch_db:purge_docs(Db, IdsRevs),
        {ok, Db2} = couch_db:reopen(Db),

        Result2 = run_query(Db2, []),
        Expect2 = {ok, [
            {meta, [{total, 2}, {offset, 0}]},
            {row, [{id, <<"3">>}, {key, 3}, {value, 3}]},
            {row, [{id, <<"4">>}, {key, 4}, {value, 4}]}
        ]},
        ?assertEqual(Expect2, Result2),

        ok
    end).

run_query(Db, Opts) ->
    couch_mrview:query_view(Db, <<"_design/bar">>, <<"baz">>, Opts).


get_rev(#full_doc_info{} = FDI) ->
    #doc_info{
        revs = [#rev_info{} = PrevRev | _]
    } = couch_doc:to_doc_info(FDI),
    PrevRev#rev_info.rev.

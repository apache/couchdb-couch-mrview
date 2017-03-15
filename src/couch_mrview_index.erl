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

-module(couch_mrview_index).


-export([get/2]).
-export([init/2, open/2, close/1, reset/1, delete/1]).
-export([start_update/3, purge/4, process_doc/3, finish_update/1, commit/1]).
-export([compact/3, swap_compacted/2]).
-export([index_file_exists/1]).
-export([update_local_purge_doc/2, verify_index_exists/1]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


get(Property, State) ->
    case Property of
        db_name ->
            State#mrst.db_name;
        idx_name ->
            State#mrst.idx_name;
        signature ->
            State#mrst.sig;
        update_seq ->
            State#mrst.update_seq;
        purge_seq ->
            State#mrst.purge_seq;
        update_options ->
            Opts = State#mrst.design_opts,
            IncDesign = couch_util:get_value(<<"include_design">>, Opts, false),
            LocalSeq = couch_util:get_value(<<"local_seq">>, Opts, false),
            SeqIndexed = couch_util:get_value(<<"seq_indexed">>, Opts, false),
            KeySeqIndexed = couch_util:get_value(<<"keyseq_indexed">>, Opts, false),
            if IncDesign -> [include_design]; true -> [] end
                ++ if LocalSeq -> [local_seq]; true -> [] end
                ++ if KeySeqIndexed -> [keyseq_indexed]; true -> [] end
                ++ if SeqIndexed -> [seq_indexed]; true -> [] end;
        fd ->
            State#mrst.fd;
        language ->
            State#mrst.language;
        views ->
            State#mrst.views;
        info ->
            #mrst{
                fd = Fd,
                sig = Sig,
                id_btree = IdBtree,
                log_btree = LogBtree,
                language = Lang,
                update_seq = UpdateSeq,
                purge_seq = PurgeSeq,
                views = Views,
                design_opts = Opts
            } = State,
            {ok, FileSize} = couch_file:bytes(Fd),
            {ok, ExternalSize} = couch_mrview_util:calculate_external_size(Views),
            LogBtSize = case LogBtree of
                nil ->
                    0;
                _ ->
                    couch_btree:size(LogBtree)
            end,
            ActiveSize = couch_btree:size(IdBtree) + LogBtSize + ExternalSize,

            IncDesign = couch_util:get_value(<<"include_design">>, Opts, false),
            LocalSeq = couch_util:get_value(<<"local_seq">>, Opts, false),
            SeqIndexed = couch_util:get_value(<<"seq_indexed">>, Opts, false),
            KeySeqIndexed = couch_util:get_value(<<"keyseq_indexed">>, Opts, false),
            UpdateOptions =
                if IncDesign -> [<<"include_design">>]; true -> [] end
                ++ if LocalSeq -> [<<"local_seq">>]; true -> [] end
                ++ if KeySeqIndexed -> [<<"keyseq_indexed">>]; true -> [] end
                ++ if SeqIndexed -> [<<"seq_indexed">>]; true -> [] end,


            {ok, [
                {signature, list_to_binary(couch_index_util:hexsig(Sig))},
                {language, Lang},
                {disk_size, FileSize}, % legacy
                {data_size, ExternalSize}, % legacy
                {sizes, {[
                    {file, FileSize},
                    {active, ActiveSize},
                    {external, ExternalSize}
                ]}},
                {update_seq, UpdateSeq},
                {purge_seq, PurgeSeq},
                {update_options, UpdateOptions}
            ]};
        Other ->
            throw({unknown_index_property, Other})
    end.


init(Db, DDoc) ->
    couch_mrview_util:ddoc_to_mrst(couch_db:name(Db), DDoc).


open(Db, State) ->
    #mrst{
        db_name=DbName,
        sig=Sig
    } = State,
    IndexFName = couch_mrview_util:index_file(DbName, Sig),

    % If we are upgrading from <=1.2.x, we upgrade the view
    % index file on the fly, avoiding an index reset.
    %
    % OldSig is `ok` if no upgrade happened.
    %
    % To remove suppport for 1.2.x auto-upgrades in the
    % future, just remove the next line and the code
    % between "upgrade code for <= 1.2.x" and
    % "end upgrade code for <= 1.2.x" and the corresponding
    % code in couch_mrview_util

    OldSig = couch_mrview_util:maybe_update_index_file(State),

    case couch_mrview_util:open_file(IndexFName) of
        {ok, Fd} ->
            case (catch couch_file:read_header(Fd)) of
                % upgrade code for <= 1.2.x
                {ok, {OldSig, Header}} ->
                    % Matching view signatures.
                    NewSt = couch_mrview_util:init_state(Db, Fd, State, Header),
                    maybe_create_local_purge_doc(Db, NewSt),
                    {ok, NewSt};
                % end of upgrade code for <= 1.2.x
                {ok, {Sig, Header}} ->
                    % Matching view signatures.
                    NewSt = couch_mrview_util:init_state(Db, Fd, State, Header),
                    maybe_create_local_purge_doc(Db, NewSt),
                    {ok, NewSt};
                _ ->
                    NewSt = couch_mrview_util:reset_index(Db, Fd, State),
                    maybe_create_local_purge_doc(Db, NewSt),
                    {ok, NewSt}
            end;
        {error, Reason} = Error ->
            couch_log:error("Failed to open view file '~s': ~s",
                            [IndexFName, file:format_error(Reason)]),
            Error
    end.


close(State) ->
    erlang:demonitor(State#mrst.fd_monitor, [flush]),
    couch_file:close(State#mrst.fd).


delete(#mrst{db_name=DbName, sig=Sig}=State) ->
    couch_file:close(State#mrst.fd),
    catch couch_mrview_util:delete_files(DbName, Sig).


reset(State) ->
    couch_util:with_db(State#mrst.db_name, fun(Db) ->
        NewState = couch_mrview_util:reset_index(Db, State#mrst.fd, State),
        {ok, NewState}
    end).


start_update(PartialDest, State, NumChanges) ->
    couch_mrview_updater:start_update(PartialDest, State, NumChanges).


purge(Db, PurgeSeq, PurgedIdRevs, State) ->
    couch_mrview_updater:purge(Db, PurgeSeq, PurgedIdRevs, State).


process_doc(Doc, Seq, State) ->
    couch_mrview_updater:process_doc(Doc, Seq, State).


finish_update(State) ->
    couch_mrview_updater:finish_update(State).


commit(State) ->
    Header = {State#mrst.sig, couch_mrview_util:make_header(State)},
    couch_file:write_header(State#mrst.fd, Header).


compact(Db, State, Opts) ->
    couch_mrview_compactor:compact(Db, State, Opts).


swap_compacted(OldState, NewState) ->
    couch_mrview_compactor:swap_compacted(OldState, NewState).


index_file_exists(State) ->
    #mrst{
        db_name=DbName,
        sig=Sig
    } = State,
    IndexFName = couch_mrview_util:index_file(DbName, Sig),
    filelib:is_file(IndexFName).


update_local_purge_doc(Db, State) ->
    Sig = couch_index_util:hexsig(get(signature, State)),
    Doc = couch_doc:from_json_obj({[
        {<<"_id">>, couch_mrview_util:get_local_purge_doc_id(Sig)},
        {<<"purge_seq">>, get(purge_seq, State)},
        {<<"timestamp_utc">>, list_to_binary(couch_util:utc_string())},
        {<<"verify_module">>, <<"couch_mrview_index">>},
        {<<"verify_function">>, <<"verify_index_exists">>},
        {<<"verify_options">>, {[
            {<<"dbname">>, get(db_name, State)},
            {<<"ddoc_id">>, get(idx_name, State)},
            {<<"signature">>, Sig}
        ]}},
        {<<"type">>, <<"mrview">>}
    ]}),
    couch_db:update_doc(Db, Doc, []).


verify_index_exists(Options) ->
    ShardDbName = couch_mrview_util:get_value_from_options(<<"dbname">>, Options),
    DDocId = couch_mrview_util:get_value_from_options(<<"ddoc_id">>, Options),
    SigInLocal = couch_mrview_util:get_value_from_options(<<"signature">>, Options),
    case couch_db:open_int(ShardDbName, []) of
        {ok, Db} ->
            try
                DbName = mem3:dbname(couch_db:name(Db)),
                case ddoc_cache:open(DbName, DDocId) of
                    {ok, DDoc} ->
                        {ok, IdxState} = couch_mrview_util:ddoc_to_mrst(ShardDbName, DDoc),
                        couch_index_util:hexsig(IdxState#mrst.sig) == SigInLocal;
                    _Else ->
                        false
                end
            catch E:T ->
                Stack = erlang:get_stacktrace(),
                couch_log:error("Error occurs when verifying existence of ~s/~s :: ~p ~p",
                    [ShardDbName, DDocId, {E, T}, Stack]),
                false
            after
                catch couch_db:close(Db)
            end;
        _ ->
            false
    end.


maybe_create_local_purge_doc(Db, State) ->
    Sig = couch_index_util:hexsig(get(signature, State)),
    LocalPurgeDocId = couch_mrview_util:get_local_purge_doc_id(Sig),
    case couch_db:open_doc(Db, LocalPurgeDocId, []) of
        {not_found, _Reason} ->
            update_local_purge_doc(Db, State);
        {ok, _LocalPurgeDoc} ->
            ok
    end.

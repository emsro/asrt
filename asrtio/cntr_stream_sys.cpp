/// Permission to use, copy, modify, and/or distribute this software for any
/// purpose with or without fee is hereby granted.
///
/// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
/// REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
/// AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
/// INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
/// LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
/// OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
/// PERFORMANCE OF THIS SOFTWARE.
#include "./cntr_stream_sys.hpp"

namespace asrtio
{

void write_strm_field( std::ostream& os, struct asrt_stream_field_desc const* desc, uint8_t*& p )
{
        if ( desc->type == ASRT_STRM_FIELD_ARRAY ) {
                os << "[";
                for ( uint16_t i = 0; i < desc->count; i++ ) {
                        if ( i > 0 )
                                os << ",";
                        write_strm_field( os, desc->elem, p );
                }
                os << "]";
                return;
        }
        write_strm_field( os, (enum asrt_strm_field_type_e) desc->type, p );
}

void write_strm_field( std::ostream& os, enum asrt_strm_field_type_e ft, uint8_t*& p )
{
        switch ( ft ) {
        case ASRT_STRM_FIELD_U8:
                os << static_cast< unsigned >( *p++ );
                break;
        case ASRT_STRM_FIELD_U16: {
                uint16_t v;
                asrt_cut_u16( &p, &v );
                os << v;
                break;
        }
        case ASRT_STRM_FIELD_U32: {
                uint32_t v;
                asrt_cut_u32( &p, &v );
                os << v;
                break;
        }
        case ASRT_STRM_FIELD_I8:
                os << static_cast< int >( static_cast< int8_t >( *p++ ) );
                break;
        case ASRT_STRM_FIELD_I16: {
                uint16_t uv;
                asrt_cut_u16( &p, &uv );
                os << static_cast< int16_t >( uv );
                break;
        }
        case ASRT_STRM_FIELD_I32: {
                int32_t v;
                asrt_cut_i32( &p, &v );
                os << v;
                break;
        }
        case ASRT_STRM_FIELD_FLOAT: {
                uint32_t bits;
                asrt_cut_u32( &p, &bits );
                float v;
                std::memcpy( &v, &bits, 4 );
                os << v;
                break;
        }
        case ASRT_STRM_FIELD_BOOL:
                os << ( *p++ ? "true" : "false" );
                break;
        default:
                break;
        }
}

void write_stream_csv(
    output_fs&                   fs,
    std::filesystem::path const& path,
    asrt_stream_schema const&    sc )
{
        auto  w  = fs.open_write( path );
        auto& os = w.stream();
        for ( uint8_t fi = 0; fi < sc.field_count; ++fi ) {
                if ( fi > 0 )
                        os << ",";
                os << asrt_strm_field_type_to_str(
                    (enum asrt_strm_field_type_e) sc.fields[fi].type );
        }
        os << "\n";
        for ( auto* rec = sc.first; rec; rec = rec->next ) {
                uint8_t* p = rec->data;
                for ( uint8_t fi = 0; fi < sc.field_count; ++fi ) {
                        if ( fi > 0 )
                                os << ",";
                        write_strm_field( os, &sc.fields[fi], p );
                }
                os << "\n";
        }
}

task< void > handle_stream(
    task_ctx&                    ctx,
    asrt::stream_schemas         schemas,
    suite_reporter&              reporter,
    std::string_view             name,
    output_fs&                   fs,
    std::filesystem::path const& run_dir,
    bool                         do_output )
{
        if ( schemas->schema_count == 0 )
                co_return;
        co_await reporter.on_stream_data( name, schemas );
        if ( !do_output )
                co_return;
        auto const& ss = *schemas;
        for ( uint32_t si = 0; si < ss.schema_count; ++si ) {
                auto const& sc = ss.schemas[si];
                write_stream_csv(
                    fs, run_dir / ( "stream." + std::to_string( sc.schema_id ) + ".csv" ), sc );
        }
}

task< void > handle_collect(
    task_ctx&                    ctx,
    asrt_flat_tree const*        tree,
    suite_reporter&              reporter,
    std::string_view             name,
    output_fs&                   fs,
    std::filesystem::path const& path,
    bool                         do_output )
{
        if ( !tree )
                co_return;
        co_await reporter.on_collect_data( name, tree );
        if ( !do_output )
                co_return;
        nlohmann::json j;
        if ( flat_tree_to_json( const_cast< asrt_flat_tree& >( *tree ), j ) ) {
                auto w = fs.open_write( path );
                w.stream() << j.dump( 2 ) << "\n";
        }
}

task< void > handle_diag(
    task_ctx&                    ctx,
    cntr_sys&                    sys,
    suite_reporter&              reporter,
    output_fs&                   fs,
    std::filesystem::path const& path,
    bool                         do_output )
{
        std::optional< file_writer > w;
        if ( do_output ) {
                w.emplace( fs.open_write( path ) );
                w->stream() << "file,line,extra\n";
        }
        while ( auto* rec = sys.take_diag_record() ) {
                char const* extra = rec->extra ? rec->extra : "";
                co_await reporter.on_diagnostic( rec->file, rec->line, extra );
                if ( w )
                        w->stream() << rec->file << "," << rec->line << "," << extra << "\n";
                asrt_diag_free_record( &sys.assembly().diag.alloc, rec );
        }
}

task< void > run_test_suite(
    task_ctx&                 ctx,
    cntr_sys&                 sys,
    suite_reporter&           reporter,
    std::chrono::milliseconds timeout,
    param_config const&       params,
    output_fs&                fs,
    std::filesystem::path     output_dir )
{
        co_await cntr_start{ { sys.cntr(), timeout } };

        uint32_t count = co_await cntr_query_test_count{ { sys.cntr(), timeout } };
        co_await reporter.on_count( count );

        std::set< std::string > unseen_keys;
        for ( auto const& [key, _] : params.tests )
                unseen_keys.insert( key );

        for ( uint32_t i = 0; i < count; ++i ) {
                // NOTE: Use explicit move out of the awaited tuple instead of a structured
                // binding (`auto [tid, name] = co_await ...`). With GCC 15 coroutines, the
                // hidden structured-binding holder occasionally fails to be destroyed at the
                // end of an iteration in this loop, leaking the std::string it owns and
                // tripping LeakSanitizer.
                auto        info = co_await cntr_query_test_info{ { sys.cntr(), i, timeout } };
                uint16_t    tid  = std::get< 0 >( info );
                std::string name = std::move( std::get< 1 >( info ) );

                unseen_keys.erase( name );

                auto [skip, roots] = params.runs_for( name );

                if ( skip )
                        continue;

                uint32_t const run_total = static_cast< uint32_t >( roots.size() );

                for ( uint32_t ri = 0; ri < run_total; ++ri ) {
                        co_await reporter.on_test_start( name, ri + 1, run_total );

                        auto         t0  = sys.clk().now();
                        asrt::result res = co_await cntr_assembly_exec_test{
                            { .asm_ref = sys.assembly(),
                              .tid     = tid,
                              .tree    = roots[ri] != 0 ? &params.tree : nullptr,
                              .root_id = roots[ri],
                              .timeout = timeout } };

                        bool const do_output = !output_dir.empty();
                        auto const run_dir   = output_dir / name / std::to_string( ri );

                        if ( do_output )
                                fs.create_directories( run_dir );
                        co_await handle_diag( ctx, sys, reporter, fs, run_dir / "diag.csv", do_output );
                        co_await handle_collect(
                            ctx,
                            sys.collect_tree(),
                            reporter,
                            name,
                            fs,
                            run_dir / "collect.json",
                            do_output );
                        co_await handle_stream( ctx, sys.stream_take(), reporter, name, fs, run_dir, do_output );

                        double ms     = static_cast< double >( ( sys.clk().now() - t0 ).count() );
                        bool   passed = ( res.res == ASRT_TEST_RESULT_SUCCESS );
                        co_await reporter.on_test_done( name, passed, ms, ri + 1, run_total );
                }
        }

        for ( auto const& key : unseen_keys )
                ASRT_INF_LOG(
                    "asrtio",
                    "param config: key \"%s\" does not match any device test",
                    key.c_str() );

        co_return;
}

}  // namespace asrtio

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
#pragma once
#include "../asrtio/output_fs.hpp"

#include <list>
#include <map>
#include <sstream>
#include <string>
#include <vector>

struct stub_fs : asrtio::output_fs
{
        /// All directories created (in order).
        std::vector< std::string > dirs;

        /// file path -> content (last open_write wins for same path).
        std::map< std::string, std::string > files;

        bool create_directories( std::filesystem::path const& path ) override
        {
                dirs.emplace_back( path.generic_string() );
                return true;
        }

        asrtio::file_writer open_write( std::filesystem::path const& path ) override
        {
                auto& entry  = _entries.emplace_back();
                entry.path   = path.generic_string();
                entry.stream = std::ostringstream{};
                return asrtio::file_writer{ entry.stream, *this };
        }

        void close_write( std::ostream& os ) override
        {
                for ( auto it = _entries.begin(); it != _entries.end(); ++it ) {
                        if ( &it->stream == &os ) {
                                files[it->path] = it->stream.str();
                                _entries.erase( it );
                                return;
                        }
                }
        }

private:
        struct entry
        {
                std::string        path;
                std::ostringstream stream;
        };

        std::list< entry > _entries;
};

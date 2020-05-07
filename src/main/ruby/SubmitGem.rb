require 'java'
require 'builder'
require 'rubygems/indexer.rb'
require 'securerandom'
# @todo #32:120min Gem submission implementation.
#  The implementation must receive the .gem file, unzip it, and update specs files. As a result,
#  files become available for downloading.
class SubmitGem
  java_import com.artipie.http.async.AsyncResponse
  java_import com.artipie.http.rs.RsWithStatus
  java_import com.artipie.http.rs.RsStatus
  java_import com.artipie.asto.fs.RxFile
  java_import Java::io.reactivex.Single
  java_import java.nio.file.Paths
  java_import org.slf4j.LoggerFactory
  include com.artipie.http.Slice

  @@log = LoggerFactory::getLogger("com.artipie.gem.SubmitGem")

  def initialize(storage, fs)
    @storage = storage
    @fs = fs
    @idx = "temp-gem-index"
    @gems = File.join(@idx, "gems")
    # Was index created before?
    idx_existed = File.exists?(@idx)
    Dir.mkdir(@idx) unless idx_existed
    Dir.mkdir(@gems) unless File.exists?(@gems)
    @indexer = Gem::Indexer.new(@idx, { build_modern: true })
    @indexer.generate_index unless idx_existed
  end

  def response(line, headers, body)
    @@log.debug("Requested #{line}")
    local = SecureRandom.hex(32) + ".gem"
    AsyncResponse.new(
        RxFile.new(Paths::get(@gems, local), @fs).save(body).and_then(
            Single::from_callable {
              # @todo #9:30min Sync generated indexes with Storage.
              #  For now, generated indexes are stored locally in temp-gem-index directory.
              #  Those should also be syncronized with storage.
              @indexer.update_index
              RsWithStatus.new(RsStatus::OK)
            }
        )
    )
  end
end
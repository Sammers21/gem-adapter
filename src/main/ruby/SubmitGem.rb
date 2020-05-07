require 'java'
require 'builder'
require 'rubygems/indexer.rb'
require 'securerandom'

class SubmitGem
  java_import com.artipie.http.async.AsyncResponse
  java_import com.artipie.http.rs.RsWithStatus
  java_import com.artipie.http.rs.RsStatus
  java_import com.artipie.asto.rx.RxCopy
  java_import com.artipie.asto.rx.RxStorageWrapper
  java_import com.artipie.asto.fs.FileStorage
  java_import com.artipie.asto.fs.RxFile
  java_import com.artipie.asto.Key
  java_import Java::io.reactivex.Flowable
  java_import Java::io.reactivex.Single
  java_import Java::io.reactivex.Completable
  java_import java.util.concurrent.TimeUnit
  java_import java.nio.file.Paths
  java_import java.util.ArrayList
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
    @rx_idx_local = RxStorageWrapper.new(FileStorage.new(Paths::get(@idx), @fs))
  end

  def response(line, headers, body)
    @@log.debug("Requested #{line}")
    local = SecureRandom.hex(32) + ".gem"
    rx_storage = RxStorageWrapper.new(@storage)
    AsyncResponse.new(
        RxFile.new(Paths::get(@gems, local), @fs).save(body)
            .and_then(Completable::from_action { @indexer.update_index })
            .and_then(
                files_to_sync.flatMapCompletable {
                    |keys| RxCopy.new(@rx_idx_local, keys).copy(rx_storage)
                }
            )
            .and_then(Single::just(RsWithStatus.new(RsStatus::OK)))
    )
  end

  def files_to_sync()
    @rx_idx_local.list(Key::From.new("quick"))
        .flatMapPublisher { |keys| Flowable::from_iterable(keys) }
        .mergeWith(
            Flowable::fromIterable(
                ArrayList.new(
                    [
                        Key::From.new("latest_specs.4.8"),
                        Key::From.new("latest_specs.4.8.gz"),
                        Key::From.new("prerelease_specs.4.8"),
                        Key::From.new("prerelease_specs.4.8.gz"),
                        Key::From.new("specs.4.8"),
                        Key::From.new("specs.4.8.gz")
                    ]
                )
            )
        ).to_list
  end
end
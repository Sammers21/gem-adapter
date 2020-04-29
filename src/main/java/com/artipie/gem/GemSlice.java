/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 artipie.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.artipie.gem;

import com.artipie.asto.Storage;
import com.artipie.asto.fs.FileStorage;
import com.artipie.http.Slice;
import com.artipie.http.rq.RqMethod;
import com.artipie.http.rs.RsStatus;
import com.artipie.http.rs.RsWithStatus;
import com.artipie.http.rt.RtRule;
import com.artipie.http.rt.SliceRoute;
import com.artipie.http.slice.SliceDownload;
import com.artipie.http.slice.SliceSimple;
import io.vertx.reactivex.core.file.FileSystem;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.commons.io.IOUtils;
import org.jruby.Ruby;
import org.jruby.RubyRuntimeAdapter;
import org.jruby.javasupport.JavaEmbedUtils;

/**
 * A slice, which servers gem packages.
 *
 * @todo #13:30min Initialize on first request.
 *  Currently, Ruby runtime initialization and Slice evaluation is happening during the GemSlice
 *  construction. Instead, the Ruby runtime initialization and Slice evaluation should happen
 *  on first request.
 * @checkstyle ClassDataAbstractionCouplingCheck (500 lines)
 * @checkstyle ParameterNumberCheck (500 lines)
 * @checkstyle ParameterNameCheck (500 lines)
 * @since 0.1
 */
@SuppressWarnings({"PMD.UnusedPrivateField", "PMD.SingularField"})
public final class GemSlice extends Slice.Wrap {

    /**
     * Ctor.
     *
     * @param storage The storage.
     * @param fs The file system.
     */
    public GemSlice(final Storage storage, final FileSystem fs) {
        this(storage, JavaEmbedUtils.initialize(new ArrayList<>(0)), fs);
    }

    /**
     * Ctor.
     *
     * @param storage The storage.
     * @param runtime The Jruby runtime.
     * @param fs The file system.
     */
    public GemSlice(final Storage storage, final Ruby runtime, final FileSystem fs) {
        super(
            new SliceRoute(
                new SliceRoute.Path(
                    new RtRule.Multiple(
                        new RtRule.ByMethod(RqMethod.POST),
                        new RtRule.ByPath("/api/v1/gems")
                    ),
                    GemSlice.rubyLookUp("SubmitGem", storage, runtime, fs)
                ),
                new SliceRoute.Path(
                    new RtRule.Multiple(
                        new RtRule.ByMethod(RqMethod.GET),
                        new RtRule.ByPath(GemInfo.PATH_PATTERN)
                    ),
                    new GemInfo(storage)
                ),
                new SliceRoute.Path(
                    new RtRule.ByMethod(RqMethod.GET),
                    // @todo #25:30min Use underlying storage.
                    //  Currency, local directory 'temp-gem-index' is used as a storage for serving
                    //  metadata files. This approaches has a significant amount of drawbacks, that
                    //  is why we should change it.
                    new SliceDownload(new FileStorage(Paths.get("temp-gem-index"), fs))
                ),
                new SliceRoute.Path(
                    RtRule.FALLBACK,
                    new SliceSimple(new RsWithStatus(RsStatus.NOT_FOUND))
                )
            )
        );
    }

    /**
     * Lookup an instance of slice, implemented with JRuby.
     * @param rclass The name of a slice class, implemented in JRuby.
     * @param storage The storage to pass directly to Ruby instance.
     * @param runtime The JRuby runtime.
     * @param fs The File System.
     * @return The Slice.
     */
    private static Slice rubyLookUp(final String rclass,
        final Storage storage,
        final Ruby runtime,
        final FileSystem fs) {
        try {
            final RubyRuntimeAdapter evaler = JavaEmbedUtils.newRuntimeAdapter();
            final String script = IOUtils.toString(
                GemSlice.class.getResourceAsStream(String.format("/%s.rb", rclass)),
                StandardCharsets.UTF_8
            );
            evaler.eval(runtime, script);
            return (Slice) JavaEmbedUtils.invokeMethod(
                runtime,
                evaler.eval(runtime, rclass),
                "new",
                new Object[]{storage, fs},
                Slice.class
            );
        } catch (final IOException exc) {
            throw new UncheckedIOException(exc);
        }
    }
}

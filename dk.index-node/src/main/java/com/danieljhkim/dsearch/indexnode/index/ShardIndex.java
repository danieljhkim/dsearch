package com.danieljhkim.dsearch.indexnode.index;

import com.danieljhkim.dsearch.common.config.AppConfig.FieldConfig;
import com.danieljhkim.dsearch.common.enums.FieldType;
import com.danieljhkim.dsearch.common.exception.IndexOperationException;
import com.danieljhkim.dsearch.common.exception.ParseGoneWrongException;
import com.danieljhkim.dsearch.common.model.SearchDocument;
import com.danieljhkim.dsearch.common.model.SearchHit;
import com.danieljhkim.dsearch.common.model.SearchResult;
import com.danieljhkim.dsearch.common.pagination.SortOptions;
import com.danieljhkim.dsearch.common.pagination.SortSpec;
import com.danieljhkim.dsearch.common.schema.AnalyzerConfig;
import com.danieljhkim.dsearch.common.schema.EmbeddingModelIdentity;
import com.danieljhkim.dsearch.common.schema.FieldSchema;
import com.danieljhkim.dsearch.common.schema.IndexSchema;
import com.danieljhkim.dsearch.common.schema.IndexSchemaCompatibility;
import com.danieljhkim.dsearch.common.schema.IndexSchemaStore;
import com.danieljhkim.dsearch.indexnode.index.facet.FacetCalculator;
import com.danieljhkim.dsearch.indexnode.index.highlight.TextHighlighter;
import com.danieljhkim.dsearch.indexnode.index.query.FilterQueryBuilder;
import com.danieljhkim.dsearch.indexnode.index.query.LuceneSortBuilder;
import com.danieljhkim.dsearch.ml.embedding.TextEmbedder;
import com.danieljhkim.dsearch.ml.embedding.TextEmbeddingService;
import com.danieljhkim.dsearch.proto.common.FacetRequest;
import com.danieljhkim.dsearch.proto.common.FacetResponse;
import com.danieljhkim.dsearch.proto.common.Filter;
import com.danieljhkim.dsearch.proto.common.SortValue;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import lombok.Getter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class ShardIndex implements Closeable {

    public static final String FIELD_ID = "id";
    public static final String FIELD_CONTENT = "content";
    public static final String FIELD_TITLE = "title";
    public static final String FIELD_EMBEDDING = "text_embedding";

    private static final java.util.logging.Logger LOGGER =
            java.util.logging.Logger.getLogger(ShardIndex.class.getName());

    private static final String[] DEFAULT_SEARCH_FIELDS = new String[] {FIELD_TITLE, FIELD_CONTENT};
    private static final Set<String> HIGHLIGHTABLE_FIELDS = Set.of(FIELD_TITLE, FIELD_CONTENT);

    @Getter
    private final String shardId;

    @Getter
    private final Path indexPath;

    @Getter
    private final IndexSchema schema;

    @Getter
    private final boolean serving;

    private final Directory directory;
    private final Analyzer analyzer;
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;
    private final TextEmbedder embeddingService;
    private final Closeable ownedEmbeddingService;

    // Query builders for filters, highlighting, and faceting
    private final FilterQueryBuilder filterQueryBuilder;
    private final TextHighlighter textHighlighter;
    private final FacetCalculator facetCalculator;
    private final Map<String, FieldConfig> fieldConfigMap;

    public ShardIndex(String shardId, Path baseDir) {
        this(shardId, baseDir, null);
    }

    public ShardIndex(String shardId, Path baseDir, Map<String, FieldConfig> fieldConfigMap) {
        this(shardId, baseDir, fieldConfigMap, new TextEmbeddingService(), true, null, true);
    }

    public ShardIndex(
            String shardId, Path baseDir, Map<String, FieldConfig> fieldConfigMap, TextEmbedder embeddingService) {
        this(shardId, baseDir, fieldConfigMap, embeddingService, false, null, true);
    }

    public ShardIndex(
            String shardId,
            Path baseDir,
            Map<String, FieldConfig> fieldConfigMap,
            TextEmbedder embeddingService,
            IndexSchema expectedSchema) {
        this(shardId, baseDir, fieldConfigMap, embeddingService, false, expectedSchema, true);
    }

    public ShardIndex(
            String shardId,
            Path baseDir,
            Map<String, FieldConfig> fieldConfigMap,
            TextEmbedder embeddingService,
            IndexSchema expectedSchema,
            boolean serving) {
        this(shardId, baseDir, fieldConfigMap, embeddingService, false, expectedSchema, serving);
    }

    private ShardIndex(
            String shardId,
            Path baseDir,
            Map<String, FieldConfig> fieldConfigMap,
            TextEmbedder embeddingService,
            boolean ownsEmbeddingService,
            IndexSchema expectedSchema,
            boolean serving) {
        Directory directory = null;
        Analyzer analyzer = null;
        IndexWriter indexWriter = null;
        SearcherManager searcherManager = null;
        Closeable ownedEmbeddingService = null;
        Path shardDirectory = baseDir;
        try {
            this.shardId = shardId;
            this.serving = serving;
            this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService");
            Path normalizedBaseDir = baseDir.normalize();
            Path resolved = normalizedBaseDir.resolve("shard-" + shardId).normalize();
            if (!resolved.startsWith(normalizedBaseDir)) {
                throw new IllegalArgumentException(
                        "partitionId '" + shardId + "' resolves outside the index base directory");
            }
            shardDirectory = resolved;
            this.indexPath = resolved;
            Files.createDirectories(indexPath);

            Map<String, FieldConfig> resolvedFieldConfigs = fieldConfigMap != null ? fieldConfigMap : new HashMap<>();
            IndexSchema runtimeSchema = resolveRuntimeSchema(expectedSchema, resolvedFieldConfigs, embeddingService);
            IndexSchemaStore schemaStore = new IndexSchemaStore();
            IndexSchema persistedSchema = schemaStore.load(indexPath);
            if (persistedSchema == null) {
                schemaStore.save(indexPath, runtimeSchema);
                persistedSchema = runtimeSchema;
            } else if (serving) {
                IndexSchemaCompatibility.requireCompatible(persistedSchema, runtimeSchema);
            }
            this.schema = persistedSchema;

            directory = FSDirectory.open(indexPath);
            this.directory = directory;
            analyzer = createAnalyzer(persistedSchema.analyzer().name());
            this.analyzer = analyzer;

            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriter = new IndexWriter(directory, config);
            this.indexWriter = indexWriter;

            if (!DirectoryReader.indexExists(directory)) {
                indexWriter.commit();
            }

            ownedEmbeddingService =
                    ownsEmbeddingService && embeddingService instanceof Closeable closeable ? closeable : null;
            this.ownedEmbeddingService = ownedEmbeddingService;
            DirectoryReader initialReader = DirectoryReader.open(directory);
            try {
                searcherManager = new SearcherManager(initialReader, null);
            } catch (IOException e) {
                initialReader.close();
                throw e;
            }
            this.searcherManager = searcherManager;

            // Initialize field config and query builders
            this.fieldConfigMap = resolvedFieldConfigs;
            this.filterQueryBuilder = new FilterQueryBuilder(this.fieldConfigMap);
            this.textHighlighter = new TextHighlighter();
            this.facetCalculator = new FacetCalculator();
        } catch (IOException e) {
            UnreadableShardException unreadable = UnreadableShardException.translate(shardId, shardDirectory, e);
            RuntimeException wrapped = unreadable != null
                    ? unreadable
                    : new RuntimeException("Failed to initialize ShardIndex for shard " + shardId, e);
            closeInitializingResources(
                    wrapped, searcherManager, indexWriter, directory, analyzer, ownedEmbeddingService);
            throw wrapped;
        } catch (RuntimeException e) {
            closeInitializingResources(e, searcherManager, indexWriter, directory, analyzer, ownedEmbeddingService);
            throw e;
        }
    }

    private static void closeInitializingResources(
            Throwable cause,
            SearcherManager searcherManager,
            IndexWriter indexWriter,
            Directory directory,
            Analyzer analyzer,
            Closeable ownedEmbeddingService) {
        closeInitializingResource(cause, searcherManager);
        closeInitializingResource(cause, indexWriter);
        closeInitializingResource(cause, directory);
        closeInitializingResource(cause, analyzer);
        closeInitializingResource(cause, ownedEmbeddingService);
    }

    private static void closeInitializingResource(Throwable cause, Closeable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    /**
     * Upsert a document by id.
     */
    public void index(SearchDocument doc) throws IOException {
        Document luceneDoc = toLuceneDocument(doc);
        // Wrap with FacetsConfig to properly index facet fields
        Document wrappedDoc = facetCalculator.getFacetsConfig().build(luceneDoc);
        indexWriter.updateDocument(new Term(FIELD_ID, doc.getId()), wrappedDoc);
    }

    /**
     * Delete by docId.
     */
    public void delete(String docId) throws IOException {
        indexWriter.deleteDocuments(new Term(FIELD_ID, docId));
    }

    /** Returns the current committed document for an exact id term, or {@code null} when absent. */
    public SearchDocument get(String docId) {
        IndexSearcher searcher = null;
        try {
            searcher = searcherManager.acquire();
            TopDocs topDocs = searcher.search(new TermQuery(new Term(FIELD_ID, docId)), 1);
            if (topDocs.scoreDocs.length == 0) {
                return null;
            }
            Document document = searcher.storedFields().document(topDocs.scoreDocs[0].doc);
            Map<String, String> fields = new HashMap<>();
            Set<String> seen = new HashSet<>();
            for (IndexableField field : document.getFields()) {
                String name = field.name();
                if (FIELD_ID.equals(name) || FIELD_EMBEDDING.equals(name) || !seen.add(name)) {
                    continue;
                }
                String value = document.get(name);
                if (value != null) {
                    fields.put(name, value);
                }
            }
            return new SearchDocument(docId, fields);
        } catch (IOException e) {
            throw new IndexOperationException("I/O error retrieving document from shard " + shardId, e);
        } finally {
            releaseSearcher(searcher);
        }
    }

    /**
     * Search against the current committed index state (backward compatible).
     */
    public SearchResult search(String queryString, int limit, int from) {
        return search(queryString, limit, from, null, false, null);
    }

    /**
     * Search with filters, highlighting, and facets.
     */
    public SearchResult search(
            String queryString,
            int limit,
            int from,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests) {
        return search(queryString, limit, from, filters, highlight, facetRequests, SortOptions.NONE);
    }

    /**
     * Search with filters, highlighting, facets, and an explicit ordering.
     *
     * <p>When {@code sortOptions} carries a resume point the shard collects only {@code limit}
     * hits strictly after it, so the cost of a page does not grow with how deep the traversal has
     * gone. Without one it falls back to collecting {@code limit + from} and slicing, which is what
     * legacy offset paging needs.
     */
    public SearchResult search(
            String queryString,
            int limit,
            int from,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests,
            SortOptions sortOptions) {
        return search(queryString, limit, from, filters, highlight, facetRequests, sortOptions, null);
    }

    public SearchResult search(
            String queryString,
            int limit,
            int from,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests,
            SortOptions sortOptions,
            List<String> storedFields) {
        IndexSearcher searcher = null;
        try {
            searcher = searcherManager.acquire();
            MultiFieldQueryParser parser = new MultiFieldQueryParser(DEFAULT_SEARCH_FIELDS, analyzer);
            Query textQuery = parser.parse(queryString);

            // Combine text query with filters
            Query combinedQuery = combineWithFilters(textQuery, filters);
            SortOptions effectiveSort = sortOptions == null ? SortOptions.NONE : sortOptions;
            TopDocs topDocs = collectTopDocs(searcher, combinedQuery, limit, from, effectiveSort);
            int totalHits = getTotalHits(searcher, combinedQuery);

            // Compute facets if requested
            List<FacetResponse> facets = null;
            if (facetRequests != null && !facetRequests.isEmpty()) {
                facets = facetCalculator.computeFacets(searcher, combinedQuery, facetRequests);
            }

            int sliceFrom = effectiveSort.hasSearchAfter() ? 0 : from;
            SearchResult result = buildPagedResult(
                    searcher,
                    topDocs,
                    limit,
                    sliceFrom,
                    totalHits,
                    highlight ? textQuery : null,
                    effectiveSort.spec(),
                    storedFields);
            result.setFacets(facets);
            return result;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "I/O error searching shard " + shardId, e);
            throw new IndexOperationException("I/O error on shard " + shardId, e);
        } catch (ParseException e) {
            throw new ParseGoneWrongException("Failed to parse query for shard " + shardId, e);
        } finally {
            releaseSearcher(searcher);
        }
    }

    /**
     * Compute facets for a query (separate method for facet-only computation).
     */
    public List<FacetResponse> computeFacets(Query query, List<FacetRequest> facetRequests) {
        if (facetRequests == null || facetRequests.isEmpty()) {
            return new ArrayList<>();
        }
        IndexSearcher searcher = null;
        try {
            searcher = searcherManager.acquire();
            return facetCalculator.computeFacets(searcher, query, facetRequests);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to compute facets for shard " + shardId, e);
            return new ArrayList<>();
        } finally {
            releaseSearcher(searcher);
        }
    }

    /**
     * Tokenizes sample text with the analyzer this shard actually indexes and queries with, so a
     * preview never drifts from real tokenization. The stream is cut off at {@code maxTokens}
     * rather than rejected outright, since a caller previewing tokenization benefits more from a
     * truncated-but-useful result than an error.
     */
    public AnalyzedText analyze(String text, int maxTokens) {
        List<AnalyzedToken> tokens = new ArrayList<>();
        boolean truncated = false;
        try (TokenStream tokenStream = analyzer.tokenStream(FIELD_CONTENT, text)) {
            CharTermAttribute termAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute positionAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);
            OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
            tokenStream.reset();
            int position = -1;
            while (tokenStream.incrementToken()) {
                position += positionAttribute.getPositionIncrement();
                if (tokens.size() >= maxTokens) {
                    truncated = true;
                    break;
                }
                tokens.add(new AnalyzedToken(
                        termAttribute.toString(),
                        position,
                        offsetAttribute.startOffset(),
                        offsetAttribute.endOffset()));
            }
            if (!truncated) {
                tokenStream.end();
            }
        } catch (IOException e) {
            throw new IndexOperationException("I/O error analyzing text on shard " + shardId, e);
        }
        return new AnalyzedText(tokens, truncated);
    }

    public record AnalyzedToken(String text, int position, int startOffset, int endOffset) {}

    public record AnalyzedText(List<AnalyzedToken> tokens, boolean truncated) {}

    /**
     * Semantic kNN search (backward compatible).
     */
    public SearchResult semanticSearch(String queryText, int limit, int from) {
        return semanticSearch(queryText, limit, from, null, false, null);
    }

    /**
     * Semantic kNN search with filters, highlighting, and facets.
     */
    public SearchResult semanticSearch(
            String queryText,
            int limit,
            int from,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests) {
        return semanticSearch(queryText, limit, from, filters, highlight, facetRequests, SortOptions.NONE);
    }

    /**
     * Semantic kNN search with an explicit ordering.
     *
     * <p>The ordering is applied to the kNN candidate pool, not to the whole partition: kNN
     * retrieves the nearest {@code limit + from} vectors and the sort reorders those. That is why
     * cursor pagination is refused for semantic search upstream — resuming past the end of a fixed
     * candidate pool would silently stop returning results that exist.
     */
    @SuppressWarnings("all")
    public SearchResult semanticSearch(
            String queryText,
            int limit,
            int from,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests,
            SortOptions sortOptions) {
        return semanticSearch(queryText, limit, from, filters, highlight, facetRequests, sortOptions, null);
    }

    @SuppressWarnings("all")
    public SearchResult semanticSearch(
            String queryText,
            int limit,
            int from,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests,
            SortOptions sortOptions,
            List<String> storedFields) {
        IndexSearcher searcher = null;
        try {
            float[] queryEmbedding = embeddingService.embed(queryText);
            if (queryEmbedding == null || queryEmbedding.length == 0) {
                LOGGER.warning(() -> "Empty embedding for query on shard " + shardId);
                return new SearchResult(new ArrayList<>(), 0);
            }

            // Build filter query if filters exist
            Query filterQuery = null;
            if (filters != null && !filters.isEmpty()) {
                filterQuery = filterQueryBuilder.buildQuery(filters);
                if (filterQuery instanceof MatchAllDocsQuery) {
                    filterQuery = null; // No actual filtering needed
                }
            }

            Query knnQuery = filterQuery != null
                    ? new KnnFloatVectorQuery(FIELD_EMBEDDING, queryEmbedding, limit + from, filterQuery)
                    : new KnnFloatVectorQuery(FIELD_EMBEDDING, queryEmbedding, limit + from);

            searcher = searcherManager.acquire();
            SortOptions effectiveSort = sortOptions == null ? SortOptions.NONE : sortOptions;
            TopDocs topDocs = collectTopDocs(searcher, knnQuery, limit, from, effectiveSort);
            int totalHits = getTotalHits(searcher, knnQuery);

            Query highlightQuery = null;
            if (highlight) {
                try {
                    MultiFieldQueryParser parser = new MultiFieldQueryParser(DEFAULT_SEARCH_FIELDS, analyzer);
                    highlightQuery = parser.parse(queryText);
                } catch (ParseException e) {
                    LOGGER.log(Level.WARNING, "Failed to parse query for highlighting", e);
                }
            }

            // Compute facets if requested
            List<FacetResponse> facets = null;
            if (facetRequests != null && !facetRequests.isEmpty()) {
                facets = facetCalculator.computeFacets(searcher, knnQuery, facetRequests);
            }

            SearchResult result = buildPagedResult(
                    searcher, topDocs, limit, from, totalHits, highlightQuery, effectiveSort.spec(), storedFields);
            result.setFacets(facets);
            return result;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "I/O error during semantic search on shard " + shardId, e);
            throw new IndexOperationException("I/O error on shard " + shardId, e);
        } finally {
            releaseSearcher(searcher);
        }
    }

    /**
     * Combines the main query with filter queries.
     */
    private Query combineWithFilters(Query mainQuery, List<Filter> filters) {
        if (filters == null || filters.isEmpty()) {
            return mainQuery;
        }

        Query filterQuery = filterQueryBuilder.buildQuery(filters);
        if (filterQuery instanceof MatchAllDocsQuery) {
            return mainQuery;
        }

        return new BooleanQuery.Builder()
                .add(mainQuery, BooleanClause.Occur.MUST)
                .add(filterQuery, BooleanClause.Occur.FILTER)
                .build();
    }

    /**
     * Runs the collection phase under the requested ordering.
     *
     * <p>Three shapes, in increasing specificity: unsorted keeps the existing relevance path;
     * sorted-from-the-top collects {@code limit + from} so the caller can still slice an offset
     * page; sorted-with-a-resume-point collects exactly {@code limit}, which is what makes deep
     * paging cost the same as the first page.
     */
    private TopDocs collectTopDocs(IndexSearcher searcher, Query query, int limit, int from, SortOptions sortOptions)
            throws IOException {
        if (!sortOptions.isSorted()) {
            return searcher.search(query, limit + from);
        }
        Sort sort = LuceneSortBuilder.toLuceneSort(sortOptions.spec(), FIELD_ID, fieldConfigMap);
        if (!sortOptions.hasSearchAfter()) {
            // doDocScores keeps hit.score meaningful for display even when ordering ignores it.
            return searcher.search(query, limit + from, sort, true);
        }
        int maxDoc = searcher.getIndexReader().maxDoc();
        if (maxDoc == 0) {
            // searchAfter rejects a resume marker against an empty reader, and an empty shard has
            // nothing after any position anyway.
            return new TopFieldDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0], sort.getSort());
        }
        FieldDoc after = LuceneSortBuilder.toSearchAfter(
                sortOptions.spec(), sortOptions.searchAfter(), FIELD_ID, fieldConfigMap, maxDoc);
        return searcher.searchAfter(after, query, limit, sort, true);
    }

    @SuppressWarnings("all")
    private SearchResult buildPagedResult(
            IndexSearcher searcher,
            TopDocs topDocs,
            int limit,
            int from,
            int totalHits,
            Query highlightQuery,
            SortSpec sortSpec,
            List<String> storedFields)
            throws IOException {
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        int end = Math.min(scoreDocs.length, from + limit);
        if (from >= scoreDocs.length || from >= end) {
            return new SearchResult(new ArrayList<>(), totalHits);
        }

        List<SearchHit> hits = new ArrayList<>(end - from);
        Set<String> fieldsToLoad = null;
        if (storedFields != null) {
            fieldsToLoad = new HashSet<>(storedFields);
            fieldsToLoad.add(FIELD_ID);
        }
        for (int i = from; i < end; i++) {
            ScoreDoc sd = scoreDocs[i];
            StoredFields storedField = searcher.storedFields();
            Document doc =
                    fieldsToLoad == null ? storedField.document(sd.doc) : storedField.document(sd.doc, fieldsToLoad);
            String docId = doc.get(FIELD_ID);
            if (docId == null) {
                continue;
            }

            String title = doc.get(FIELD_TITLE);
            String content = doc.get(FIELD_CONTENT);

            // Collect all other stored fields (excluding id, title, content)
            Map<String, String> fields = new HashMap<>();
            Set<String> processedFields = new HashSet<>();
            for (IndexableField field : doc.getFields()) {
                String fieldName = field.name();
                if (!FIELD_ID.equals(fieldName)
                        && !FIELD_TITLE.equals(fieldName)
                        && !FIELD_CONTENT.equals(fieldName)
                        && !processedFields.contains(fieldName)) {
                    // Get stored value - doc.get() works for all stored field types
                    String fieldValue = doc.get(fieldName);
                    if (fieldValue != null) {
                        fields.put(fieldName, fieldValue);
                        processedFields.add(fieldName);
                    }
                }
            }

            // Apply highlighting if requested
            Map<String, String> highlightedFields = null;
            if (highlightQuery != null) {
                highlightedFields = applyHighlighting(highlightQuery, title, content);
            }

            Map<String, String> fieldsMap = fields.isEmpty() ? null : fields;
            Map<String, String> highlightsMap =
                    highlightedFields != null && !highlightedFields.isEmpty() ? highlightedFields : null;
            List<SortValue> sortValues = extractSortValues(sortSpec, sd);
            hits.add(new SearchHit(docId, title, content, sd.score, highlightsMap, fieldsMap, sortValues));
        }

        return new SearchResult(hits, totalHits);
    }

    /**
     * Sort tuple for one collected hit, or null under relevance ordering.
     *
     * <p>A sorted collection always yields {@link FieldDoc}s; the defensive null keeps an
     * unexpected collector from failing the whole page, since the tuple is only used for ordering
     * and cursor issuance, both of which degrade safely.
     */
    private List<SortValue> extractSortValues(SortSpec sortSpec, ScoreDoc scoreDoc) {
        if (sortSpec == null || sortSpec.isUnsorted() || !(scoreDoc instanceof FieldDoc fieldDoc)) {
            return null;
        }
        return LuceneSortBuilder.toSortValues(sortSpec, fieldDoc, FIELD_ID, fieldConfigMap);
    }

    /**
     * Applies highlighting to the title and content fields.
     */
    private Map<String, String> applyHighlighting(Query query, String title, String content) {
        Map<String, String> fieldContents = new HashMap<>();
        if (title != null && !title.isEmpty()) {
            fieldContents.put(FIELD_TITLE, title);
        }
        if (content != null && !content.isEmpty()) {
            fieldContents.put(FIELD_CONTENT, content);
        }

        if (fieldContents.isEmpty()) {
            return null;
        }

        return textHighlighter.highlight(query, fieldContents, HIGHLIGHTABLE_FIELDS);
    }

    /**
     * Commit all pending index changes and refresh the searcher.
     */
    public void commit() throws IOException {
        indexWriter.commit();
        searcherManager.maybeRefresh();
    }

    /**
     * Installs user data for the next commit without publishing it yet. The next {@link #commit()}
     * publishes segment changes and this metadata through the same checksummed commit point.
     */
    void setLiveCommitData(Map<String, String> commitUserData) {
        indexWriter.setLiveCommitData(commitUserData.entrySet());
    }

    /** Returns user data from the latest durable Lucene commit. */
    Map<String, String> committedUserData() throws IOException {
        var commits = DirectoryReader.listCommits(directory);
        return commits.get(commits.size() - 1).getUserData();
    }

    Path indexPath() {
        return indexPath;
    }

    /**
     * Closes without committing pending writer state. Used when a commit outcome is uncertain so
     * a later close cannot accidentally publish data without its associated fencing metadata.
     */
    void rollbackAndClose() throws IOException {
        closeResources(true);
    }

    public long countDocuments() {
        IndexSearcher searcher = null;
        try {
            searcher = searcherManager.acquire();
            return getTotalHits(searcher, new MatchAllDocsQuery());
        } catch (IOException e) {
            throw new IndexOperationException("I/O error counting documents on shard " + shardId, e);
        } finally {
            releaseSearcher(searcher);
        }
    }

    public List<SearchDocument> exportDocuments() {
        IndexSearcher searcher = null;
        try {
            searcher = searcherManager.acquire();
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            List<SearchDocument> documents = new ArrayList<>(topDocs.scoreDocs.length);
            StoredFields storedFields = searcher.storedFields();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = storedFields.document(scoreDoc.doc);
                String docId = doc.get(FIELD_ID);
                if (docId == null || docId.isBlank()) {
                    continue;
                }
                Map<String, String> fields = new HashMap<>();
                Set<String> seen = new HashSet<>();
                for (IndexableField field : doc.getFields()) {
                    String fieldName = field.name();
                    if (FIELD_ID.equals(fieldName) || FIELD_EMBEDDING.equals(fieldName) || !seen.add(fieldName)) {
                        continue;
                    }
                    String value = doc.get(fieldName);
                    if (value != null) {
                        fields.put(fieldName, value);
                    }
                }
                documents.add(new SearchDocument(docId, fields));
            }
            return documents;
        } catch (IOException e) {
            throw new IndexOperationException("I/O error exporting documents on shard " + shardId, e);
        } finally {
            releaseSearcher(searcher);
        }
    }

    static Analyzer createAnalyzer(String analyzerName) {
        String name = AnalyzerConfig.normalize(analyzerName);
        return switch (name) {
            case AnalyzerConfig.KEYWORD -> new KeywordAnalyzer();
            case AnalyzerConfig.STANDARD -> new StandardAnalyzer();
            default -> throw new IllegalArgumentException("Unsupported analyzer '" + name + "'");
        };
    }

    static IndexSchema resolveRuntimeSchema(
            IndexSchema expectedSchema, Map<String, FieldConfig> fieldConfigMap, TextEmbedder embeddingService) {
        if (expectedSchema != null) {
            return expectedSchema;
        }
        EmbeddingModelIdentity identity = embeddingService.identity();
        List<FieldSchema> fields = new ArrayList<>();
        for (FieldConfig fieldConfig : fieldConfigMap.values()) {
            if (fieldConfig != null
                    && fieldConfig.getName() != null
                    && !fieldConfig.getName().isBlank()) {
                fields.add(FieldSchema.from(fieldConfig));
            }
        }
        return IndexSchema.current(AnalyzerConfig.standard(), fields, identity);
    }

    @SuppressWarnings("all")
    private Document toLuceneDocument(SearchDocument doc) throws IOException {
        Document luceneDoc = new Document();

        // 1) ID: stored, not tokenized
        luceneDoc.add(new StringField(FIELD_ID, doc.getId(), Field.Store.YES));
        // DocValues on the id back the universal sort tie-breaker. Without them no ordering could
        // be made total, and cursor pagination would repeat or drop documents that tie.
        luceneDoc.add(new SortedDocValuesField(FIELD_ID, new BytesRef(doc.getId())));

        // 2) Build combined text from all fields (excluding id)
        StringBuilder contentBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : doc.getFields().entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (value == null || value.isBlank()) {
                continue;
            }
            if (!FIELD_ID.equals(name)) {
                contentBuilder.append(value).append(' ');
            }
            if (FIELD_TITLE.equals(name)) {
                luceneDoc.add(new StoredField(FIELD_TITLE, value));
            }
            if (FIELD_CONTENT.equals(name)) {
                luceneDoc.add(new StoredField(FIELD_CONTENT, value));
            }

            // Index field based on configuration
            addConfiguredField(luceneDoc, name, value);
        }

        String combinedText = contentBuilder.toString().trim();

        // 3) Full-text field for BM25 / keyword search
        if (!combinedText.isEmpty()) {
            luceneDoc.add(new TextField(FIELD_CONTENT, combinedText, Field.Store.NO));
        }

        // 4) Embedding for semantic search
        if (!combinedText.isEmpty()) {
            float[] embedding = embeddingService.embed(combinedText);
            if (embedding != null && embedding.length > 0) {
                luceneDoc.add(new KnnFloatVectorField(FIELD_EMBEDDING, embedding));
            }
        }

        return luceneDoc;
    }

    /**
     * Adds a field to the Lucene document based on its FieldConfig.
     * This enables filtering, sorting, and faceting on configured fields.
     */
    @SuppressWarnings("all")
    private void addConfiguredField(Document luceneDoc, String fieldName, String value) {
        FieldConfig config = fieldConfigMap.get(fieldName);
        if (config == null) {
            return;
        }

        FieldType fieldType = config.getType();
        boolean filterable = config.isFilterable();
        boolean sortable = config.isSortable();
        boolean facetable = config.isFacetable();

        if (!filterable && !sortable && !facetable) {
            return;
        }

        try {
            switch (fieldType) {
                case INTEGER -> {
                    int intValue = Integer.parseInt(value);
                    if (filterable) {
                        luceneDoc.add(new IntPoint(fieldName, intValue));
                    }
                    if (sortable) {
                        luceneDoc.add(new NumericDocValuesField(fieldName, intValue));
                    }
                    if (facetable) {
                        // Term-style faceting for numeric values (e.g., year=1994). Range/histogram
                        // facets are handled separately.
                        luceneDoc.add(new SortedSetDocValuesFacetField(fieldName, value));
                    }
                    luceneDoc.add(new StoredField(fieldName, intValue));
                }
                case LONG, DATE -> {
                    long longValue = Long.parseLong(value);
                    if (filterable) {
                        luceneDoc.add(new LongPoint(fieldName, longValue));
                    }
                    if (sortable) {
                        luceneDoc.add(new NumericDocValuesField(fieldName, longValue));
                    }
                    if (facetable) {
                        // Term-style faceting for numeric values.
                        luceneDoc.add(new SortedSetDocValuesFacetField(fieldName, value));
                    }
                    luceneDoc.add(new StoredField(fieldName, longValue));
                }
                case DOUBLE -> {
                    double doubleValue = Double.parseDouble(value);
                    if (filterable) {
                        luceneDoc.add(new DoublePoint(fieldName, doubleValue));
                    }
                    if (sortable) {
                        luceneDoc.add(new DoubleDocValuesField(fieldName, doubleValue));
                    }
                    if (facetable) {
                        // Term-style faceting for numeric values.
                        luceneDoc.add(new SortedSetDocValuesFacetField(fieldName, value));
                    }
                    luceneDoc.add(new StoredField(fieldName, doubleValue));
                }
                case STRING -> {
                    if (filterable) {
                        luceneDoc.add(new StringField(fieldName, value, Field.Store.YES));
                    } else {
                        // Store value even when not filterable (needed for retrieval in fields map)
                        luceneDoc.add(new StoredField(fieldName, value));
                    }
                    if (sortable) {
                        luceneDoc.add(new SortedDocValuesField(fieldName, new BytesRef(value)));
                    }
                    if (facetable) {
                        // Add facet field for faceting
                        luceneDoc.add(new SortedSetDocValuesFacetField(fieldName, value));
                    }
                }
            }
        } catch (NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Failed to parse field " + fieldName + " with value " + value, e);
        }
    }

    private int getTotalHits(IndexSearcher searcher, Query query) throws IOException {
        TotalHitCountCollector countCollector = new TotalHitCountCollector();
        searcher.search(query, countCollector);
        return countCollector.getTotalHits();
    }

    private void releaseSearcher(IndexSearcher searcher) {
        if (searcher != null) {
            try {
                searcherManager.release(searcher);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to release searcher for shard " + shardId, e);
            }
        }
    }

    @Override
    @SuppressWarnings("all")
    public void close() throws IOException {
        closeResources(false);
    }

    private void closeResources(boolean rollback) throws IOException {
        IOException first = null;
        try {
            searcherManager.close();
        } catch (IOException e) {
            first = e;
        }
        try {
            if (rollback) {
                indexWriter.rollback();
            } else {
                indexWriter.close();
            }
        } catch (IOException e) {
            first = collectCloseFailure(first, e);
        }
        try {
            directory.close();
        } catch (IOException e) {
            first = collectCloseFailure(first, e);
        }
        try {
            analyzer.close();
        } catch (Exception e) {
            IOException closeFailure = e instanceof IOException ioException
                    ? ioException
                    : new IOException("Failed to close analyzer for shard " + shardId, e);
            first = collectCloseFailure(first, closeFailure);
        }
        try {
            if (ownedEmbeddingService != null) {
                ownedEmbeddingService.close();
            }
        } catch (IOException e) {
            first = collectCloseFailure(first, e);
        }
        if (first != null) {
            throw first;
        }
    }

    private IOException collectCloseFailure(IOException first, IOException failure) {
        if (first == null) {
            return failure;
        }
        first.addSuppressed(failure);
        LOGGER.log(Level.WARNING, "Suppressed exception while closing resources for shard " + shardId, failure);
        return first;
    }
}

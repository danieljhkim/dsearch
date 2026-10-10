package com.danieljhkim.dsearch.common.pagination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.dsearch.common.enums.FieldType;
import com.danieljhkim.dsearch.common.schema.AnalyzerConfig;
import com.danieljhkim.dsearch.common.schema.EmbeddingModelIdentity;
import com.danieljhkim.dsearch.common.schema.FieldSchema;
import com.danieljhkim.dsearch.common.schema.IndexSchema;
import com.danieljhkim.dsearch.proto.common.SortOrder;
import com.danieljhkim.dsearch.proto.common.SortValue;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class SortSpecTest {

    private static final IndexSchema SCHEMA = IndexSchema.current(
            AnalyzerConfig.standard(),
            List.of(
                    new FieldSchema("price", FieldType.DOUBLE, true, true, false, false, "standard"),
                    new FieldSchema("year", FieldType.INTEGER, true, true, false, false, "standard"),
                    new FieldSchema("body", FieldType.STRING, false, false, false, true, "standard")),
            EmbeddingModelIdentity.of("model-a", "PyTorch", 8));

    @Test
    void appendsTheDocumentIdTieBreakerToEveryOrdering() {
        SortSpec spec = SortSpec.effective(List.of(field("price", SortOrder.SORT_ORDER_DESC)), false);

        assertEquals(2, spec.size());
        assertEquals("price", spec.components().get(0).field());
        assertTrue(spec.components().get(0).descending());
        assertEquals(SortSpec.DOC_ID_FIELD, spec.components().get(1).field());
        assertFalse(spec.components().get(1).descending());
    }

    @Test
    void honoursAnExplicitDocumentIdComponentAndDropsAnythingAfterIt() {
        SortSpec spec = SortSpec.effective(
                List.of(
                        field("year", SortOrder.SORT_ORDER_ASC),
                        field(SortSpec.DOC_ID_FIELD, SortOrder.SORT_ORDER_DESC),
                        field("price", SortOrder.SORT_ORDER_ASC)),
                false);

        // _id is unique, so a component after it could never be consulted.
        assertEquals(2, spec.size());
        assertEquals(SortSpec.DOC_ID_FIELD, spec.components().get(1).field());
        assertTrue(spec.components().get(1).descending());
    }

    @Test
    void keepsTheFirstDirectionWhenAFieldIsRepeated() {
        SortSpec spec = SortSpec.effective(
                List.of(field("price", SortOrder.SORT_ORDER_DESC), field("price", SortOrder.SORT_ORDER_ASC)), false);

        assertEquals(2, spec.size());
        assertTrue(spec.components().get(0).descending());
    }

    @Test
    void anEmptyRequestStaysUnsortedUnlessACursorNeedsAResumableOrder() {
        assertTrue(SortSpec.effective(List.of(), false).isUnsorted());

        SortSpec forCursor = SortSpec.effective(List.of(), true);
        assertEquals(
                List.of(SortSpec.SCORE_FIELD, SortSpec.DOC_ID_FIELD),
                forCursor.components().stream()
                        .map(SortSpec.SortComponent::field)
                        .toList());
    }

    @Test
    void rejectsUnknownAndNonSortableFields() {
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> SortSpec.effective(List.of(field("nope", SortOrder.SORT_ORDER_ASC)), false)
                                .validateAgainst(SCHEMA))
                .getMessage()
                .contains("Unknown sort field"));

        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> SortSpec.effective(List.of(field("body", SortOrder.SORT_ORDER_ASC)), false)
                                .validateAgainst(SCHEMA))
                .getMessage()
                .contains("not sortable"));
    }

    @Test
    void pseudoFieldsAreAlwaysEligible() {
        SortSpec.effective(List.of(field(SortSpec.SCORE_FIELD, SortOrder.SORT_ORDER_DESC)), false)
                .validateAgainst(SCHEMA);
        SortSpec.effective(List.of(field(SortSpec.DOC_ID_FIELD, SortOrder.SORT_ORDER_ASC)), false)
                .validateAgainst(SCHEMA);
    }

    @Test
    void canonicalFormDistinguishesFieldsAndDirections() {
        String ascending = SortSpec.effective(List.of(field("price", SortOrder.SORT_ORDER_ASC)), false)
                .canonicalForm();
        String descending = SortSpec.effective(List.of(field("price", SortOrder.SORT_ORDER_DESC)), false)
                .canonicalForm();
        String otherField = SortSpec.effective(List.of(field("year", SortOrder.SORT_ORDER_ASC)), false)
                .canonicalForm();

        assertEquals(
                3,
                List.of(ascending, descending, otherField).stream().distinct().count());
    }

    @Test
    void orderingIsAppliedComponentByComponentInDeclaredOrder() {
        SortSpec spec = SortSpec.effective(
                List.of(field("year", SortOrder.SORT_ORDER_DESC), field("price", SortOrder.SORT_ORDER_ASC)), false);
        Comparator<List<SortValue>> comparator = spec.tupleComparator();

        // Newer year wins outright.
        assertTrue(comparator.compare(
                        tuple(SortValues.of(2024L), SortValues.of(9.0), SortValues.of("a")),
                        tuple(SortValues.of(2020L), SortValues.of(1.0), SortValues.of("a")))
                < 0);
        // Same year: cheaper price wins.
        assertTrue(comparator.compare(
                        tuple(SortValues.of(2024L), SortValues.of(1.0), SortValues.of("z")),
                        tuple(SortValues.of(2024L), SortValues.of(9.0), SortValues.of("a")))
                < 0);
        // Same year and price: the id tie-breaker still separates them, so no two hits ever tie.
        assertTrue(comparator.compare(
                        tuple(SortValues.of(2024L), SortValues.of(1.0), SortValues.of("a")),
                        tuple(SortValues.of(2024L), SortValues.of(1.0), SortValues.of("b")))
                < 0);
    }

    @Test
    void missingValuesOrderLastInBothDirections() {
        for (SortOrder order : List.of(SortOrder.SORT_ORDER_ASC, SortOrder.SORT_ORDER_DESC)) {
            Comparator<List<SortValue>> comparator =
                    SortSpec.effective(List.of(field("price", order)), false).tupleComparator();

            assertTrue(
                    comparator.compare(
                                    tuple(SortValues.missing(), SortValues.of("a")),
                                    tuple(SortValues.of(1.0), SortValues.of("a")))
                            > 0,
                    "missing must order last when " + order);
            assertEquals(
                    0,
                    comparator.compare(
                            tuple(SortValues.missing(), SortValues.of("a")),
                            tuple(SortValues.missing(), SortValues.of("a"))));
        }
    }

    @Test
    void aTruncatedTupleOrdersLastRatherThanFailingTheMerge() {
        SortSpec spec = SortSpec.effective(List.of(field("price", SortOrder.SORT_ORDER_ASC)), false);
        Comparator<List<SortValue>> comparator = spec.tupleComparator();

        assertTrue(comparator.compare(List.of(), tuple(SortValues.of(1.0), SortValues.of("a"))) > 0);
        assertTrue(comparator.compare(null, tuple(SortValues.of(1.0), SortValues.of("a"))) > 0);
    }

    @Test
    void stringsCompareAsUtf8BytesToMatchLuceneRatherThanUtf16() {
        // U+FF00 encodes to bytes above the surrogate range; Java's UTF-16 String.compareTo puts a
        // supplementary character before it, while Lucene's BytesRef order puts it after.
        String supplementary = new String(Character.toChars(0x10000));
        String fullWidth = "＀";

        assertTrue(supplementary.compareTo(fullWidth) < 0, "premise: UTF-16 order puts the supplementary first");
        assertTrue(
                SortValues.compareUtf8(supplementary, fullWidth) > 0,
                "UTF-8 byte order must disagree, which is why the merge cannot use String.compareTo");
    }

    private static com.danieljhkim.dsearch.proto.common.SortField field(String name, SortOrder order) {
        return com.danieljhkim.dsearch.proto.common.SortField.newBuilder()
                .setField(name)
                .setOrder(order)
                .build();
    }

    private static List<SortValue> tuple(SortValue... values) {
        return List.of(values);
    }
}

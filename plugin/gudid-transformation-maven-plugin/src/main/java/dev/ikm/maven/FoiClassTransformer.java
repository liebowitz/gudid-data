package dev.ikm.maven;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.assembler.SemanticAssembler;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_PATTERN;
import static dev.ikm.tinkar.terms.TinkarTerm.DEVELOPMENT_PATH;
import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER;

public class FoiClassTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(FoiClassTransformer.class.getSimpleName());

    // Column indices for foiclass.txt
    private static final int REVIEW_PANEL = 0;
    private static final int MEDICAL_SPECIALTY = 1;
    private static final int PRODUCT_CODE = 2;
    private static final int DEVICE_NAME = 3;


    // Hard-coded UUIDs for parent medical specialty concepts (placeholders for now)
    private static final Map<String, UUID> MEDICAL_SPECIALTY_CONCEPT_UUIDS = new HashMap<>();

    static {
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Anesthesiology", UUID.fromString("0e0ac17d-61a3-4f57-af96-63bdb26a9a81"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Cardiovascular", UUID.fromString("58019993-2366-4fae-8f1f-e7d480a4ab07"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Clinical Chemistry", UUID.fromString("6b786704-71c4-4983-ae51-d819031fdbad"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Dental", UUID.fromString("bcae0826-06ea-4cf4-8113-98ccaccfe8ae"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Ear, Nose, & Throat", UUID.fromString("48ceb40c-425f-4141-ab26-9ac7d85aaf08"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Gastroenterology & Urology", UUID.fromString("08756535-fb8e-4686-afa9-f87a71a842dd"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("General Hospital", UUID.fromString("6a6eb514-355a-4389-8bfd-f85a71b58ed2"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Hematology", UUID.fromString("67497cb4-81c2-4165-94d3-a88fb19b864a"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Immunology", UUID.fromString("adee041f-7900-4fea-9a30-6dc80346ecd6"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Microbiology", UUID.fromString("81ba5654-256d-4d2e-b24a-6099badc64d1"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Neurology", UUID.fromString("2d287625-f601-4ced-be31-076c415d4551"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Obstetrics/Gynecology", UUID.fromString("7f3aadb6-3efc-463f-aede-2171bab4c406"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Ophthalmic", UUID.fromString("4b562be9-e4d1-4aea-ba5b-a2afadd70126"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Orthopedic", UUID.fromString("868c7147-d1aa-424e-9fa4-0185d7f5f935"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Pathology", UUID.fromString("448005cc-b2d8-4fd1-810a-6ed4a99593ba"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Physical Medicine", UUID.fromString("cacd07e6-e670-41df-a757-bd33bf950f37"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Radiology", UUID.fromString("064e60ee-a604-44fc-9e34-c0a4f0aac1b7"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("General & Plastic Surgery", UUID.fromString("7ba1ba77-a9a2-45dc-936b-538a4c3e3a4b"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Clinical Toxicology", UUID.fromString("b7968a5c-717a-4526-9d07-3b8d69176286"));
        MEDICAL_SPECIALTY_CONCEPT_UUIDS.put("Unknown Medical Specialty", UUID.fromString("e6a71f02-1ae4-4b4f-a2ae-4fb11b9e1531"));
    }
    public FoiClassTransformer(UUID namespace) {
        super(namespace);
    }

    /**
     * Transforms foiclass.txt file into FDA Product Code concepts
     * @param inputFile foiclass.txt input file
     * @param composer Composer for creating concepts
     */
    @Override
    public void transform(File inputFile, Composer composer) {
        if (inputFile == null || !inputFile.exists() || !inputFile.isFile()) {
            throw new RuntimeException("FoiClass input file is either null or invalid: " +
                    (inputFile != null ? inputFile.getAbsolutePath() : "null"));
        }

        LOG.info("Starting transformation of foiclass.txt file: " + inputFile.getName());

        EntityProxy.Concept author = gudidUtility.getAuthorConcept();
        EntityProxy.Concept path = DEVELOPMENT_PATH;
        EntityProxy.Concept module = gudidUtility.getModuleConcept();

        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger skippedCount = new AtomicInteger(0);

        try (Stream<String> lines = Files.lines(inputFile.toPath())) {
            lines.skip(1) // skip header line
                    .map(row -> row.split("\\|", -1)) // -1 to preserve empty trailing fields
                    .forEach(data -> {
                        try {
                            if (data.length < 4) {
                                LOG.warn("Insufficient columns in row, expected at least 4, got {}: {}",
                                        data.length, String.join("|", data));
                                skippedCount.incrementAndGet();
                                return;
                            }

                            String medicalSpecialty = data[MEDICAL_SPECIALTY];
                            String productCode = data[PRODUCT_CODE];
                            String deviceName = data[DEVICE_NAME];

                            // Validate required fields
                            if (isEmptyOrNull(productCode)) {
                                LOG.warn("Empty or null PRODUCTCODE found in row: {}", String.join("|", data));
                                skippedCount.incrementAndGet();
                                return;
                            }

                            if (isEmptyOrNull(deviceName)) {
                                LOG.warn("Empty or null DEVICENAME found for PRODUCTCODE '{}' in row: {}",
                                        productCode, String.join("|", data));
                                skippedCount.incrementAndGet();
                                return;
                            }

                            // Create session with ACTIVE state
                            Session session = composer.open(State.ACTIVE, author, module, path);

                            // Create the FDA Product Code concept
                            createFdaProductCodeConcept(session, medicalSpecialty, productCode, deviceName);

                            processedCount.incrementAndGet();

                        } catch (Exception e) {
                            LOG.error("Error processing row: " + String.join("|", data), e);
                            skippedCount.incrementAndGet();
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("Error reading foiclass.txt file: " + inputFile.getAbsolutePath(), e);
        }

        LOG.info("Completed transformation of foiclass.txt. Processed: {}, Skipped: {}",
                processedCount.get(), skippedCount.get());
        gudidUtility.logMappingStatus();
    }

    private void createFdaProductCodeConcept(Session session, String medicalSpecialty,
                                             String productCode, String deviceName) {

        // Generate UUID for this FDA Product Code concept
        UUID conceptUuid = UuidT5Generator.get(namespace, "FDA_PRODUCT_CODE_" + productCode);
        EntityProxy.Concept fdaProductCodeConcept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));

        // Create the concept with identifiers and stated definition
        session.compose((ConceptAssembler conceptAssembler) -> conceptAssembler
                .concept(fdaProductCodeConcept)

                // Add UUID identifier
                .attach((Identifier identifier) -> identifier
                        .source(UNIVERSALLY_UNIQUE_IDENTIFIER)
                        .identifier(conceptUuid.toString())
                )
        );

        createStatedAxiom(session, fdaProductCodeConcept, medicalSpecialty);

        // Create Fully Qualified Name semantic (DEVICENAME)
        createDescriptionSemantic(session, fdaProductCodeConcept, deviceName,
                FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE);

        // Create Regular Name semantic (PRODUCTCODE)
        createDescriptionSemantic(session, fdaProductCodeConcept, productCode,
                REGULAR_NAME_DESCRIPTION_TYPE);

        // Store mapping for use by ProductCodes.txt transformation
        gudidUtility.addProductCodeMapping(productCode, conceptUuid);

        LOG.debug("Created FDA Product Code concept: '{}' ({}), Parent: {}",
                deviceName, productCode, getParentConceptName(medicalSpecialty));
    }

    private void createStatedAxiom(Session session, EntityProxy.Concept concept, String medicalSpecialty) {
        EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + medicalSpecialty + "AXIOM")));
        // Get parent concept based on medical specialty
        EntityProxy.Concept parentConcept = getParentConcept(medicalSpecialty);
        try {
            session.compose(new StatedAxiom()
                            .semantic(axiomSemantic)
                            .isA(parentConcept),
                    concept);
        } catch (Exception e) {
            LOG.error("Error creating state definition semantic for concept: " + concept, e);
        }
    }

    private void createDescriptionSemantic(Session session, EntityProxy.Concept concept,
                                           String description, EntityProxy.Concept descriptionType) {
        String typeStr = descriptionType.equals(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE) ? "FQN" :
                descriptionType.equals(REGULAR_NAME_DESCRIPTION_TYPE) ? "Regular" : "Definition";

        EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                PublicIds.of(UuidT5Generator.get(namespace,
                        concept.publicId().asUuidArray()[0] + description + typeStr + "DESC")));

        try {
            session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                    .semantic(semantic)
                    .pattern(DESCRIPTION_PATTERN)
                    .reference(concept)
                    .fieldValues(fieldValues -> fieldValues
                            .with(ENGLISH_LANGUAGE)
                            .with(description)
                            .with(DESCRIPTION_NOT_CASE_SENSITIVE)
                            .with(descriptionType)
                    ));
        } catch (Exception e) {
            LOG.error("Error creating " + typeStr + " description semantic for concept: " + concept, e);
        }
    }

    private EntityProxy.Concept getParentConcept(String medicalSpecialty) {
        // Handle empty or null medical specialty
        if (isEmptyOrNull(medicalSpecialty)) {
            LOG.debug("Empty medical specialty found, using Unknown Medical Specialty");
            return EntityProxy.Concept.make(
                    PublicIds.of(MEDICAL_SPECIALTY_CONCEPT_UUIDS.get("Unknown Medical Specialty")));
        }

        String parentConceptName = gudidUtility.getMedicalSpecialtyFullName(medicalSpecialty);
        UUID parentUuid = MEDICAL_SPECIALTY_CONCEPT_UUIDS.get(parentConceptName);

        if (parentUuid == null) {
            LOG.warn("No UUID mapping found for medical specialty: '{}', using Unknown Medical Specialty",
                    parentConceptName);
            parentUuid = MEDICAL_SPECIALTY_CONCEPT_UUIDS.get("Unknown Medical Specialty");
        }

        return EntityProxy.Concept.make(PublicIds.of(parentUuid));
    }

    private String getParentConceptName(String medicalSpecialty) {
        if (isEmptyOrNull(medicalSpecialty)) {
            return "Unknown Medical Specialty";
        }
        return gudidUtility.getMedicalSpecialtyFullName(medicalSpecialty);
    }

    private boolean isEmptyOrNull(String value) {
        return value == null || value.trim().isEmpty();
    }
}

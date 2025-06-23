package dev.ikm.maven;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class DeviceTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DeviceTransformer.class.getSimpleName());

    private static final int PRIMARY_DI = 0;
    private static final int PUBLIC_DEVICE_RECORD_KEY = 1;
    private static final int DEVICE_RECORD_STATUS = 3;
    private static final int DEVICE_PUBLISH_DATE = 6;
    private static final int BRAND_NAME = 9;
    private static final int VERSION_MODEL_NUMBER = 10;

    public DeviceTransformer(UUID namespace) {
        super(namespace);
    }

    /**
     * transforms concept file into entity
     *
     * @param inputFile concept input txt file
     */
    @Override
    public void transform(File inputFile, Composer composer) {
        if (inputFile == null || !inputFile.exists() || !inputFile.isFile()) {
            throw new RuntimeException("Concept input file is either null or invalid.");
        }

        AtomicInteger conceptCount = new AtomicInteger();
        try (Stream<String> lines = Files.lines(inputFile.toPath())) {
            lines.skip(1) //skip first line, i.e. header line
                    .map(row -> row.split("\\|"))
                    .forEach(data -> {
                        State status = "Published".equals(data[DEVICE_RECORD_STATUS]) ? State.ACTIVE : State.INACTIVE;
                        long time = LocalDate.parse(data[DEVICE_PUBLISH_DATE]).atStartOfDay().atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                        Session session = composer.open(status, time, gudidUtility.getAuthorConcept(),
                                gudidUtility.getModuleConcept(), TinkarTerm.DEVELOPMENT_PATH);

                        EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, data[PRIMARY_DI])));

                        session.compose((ConceptAssembler conceptAssembler) -> conceptAssembler
                                .concept(concept)
                                .attach((Identifier identifier) -> identifier
                                        .source(TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER)
                                        .identifier(concept.asUuidArray()[0].toString())
                                )
                                .attach((FullyQualifiedName fqn) -> fqn
                                        .language(TinkarTerm.ENGLISH_LANGUAGE)
                                        .text(data[BRAND_NAME] + " " + data[VERSION_MODEL_NUMBER])
                                        .caseSignificance(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                                )
                                .attach((Identifier identifier) -> identifier
                                        .source(gudidUtility.getPublicDeviceRecordKeyConcept())
                                        .identifier(data[PUBLIC_DEVICE_RECORD_KEY])
                                )
                        );

                        if (conceptCount.incrementAndGet() % 1000 == 0) {
                            LOG.debug("conceptCount: {} componentsInSessionCount: {}", conceptCount.get(), session.componentsInSessionCount());
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            LOG.debug("conceptCount: {}", conceptCount.get());
        }
    }

}

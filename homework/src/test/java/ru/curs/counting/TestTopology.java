package test.java.ru.curs.homework;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.homework.configuration.KafkaConfiguration;
import ru.curs.homework.configuration.TopologyConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;
import static ru.curs.counting.model.TopicNames.*;

public class TestTopology {

    private TestInputTopic<String, Bet> inputTopic;
    private TestInputTopic<String, EventScore> scoreTopic;
    private TestOutputTopic<String, Fraud> fraudTopic;
    private TestOutputTopic<String, Long> commandOutputTopic;
    private TestOutputTopic<String, Long> bettorOutputTopic;
    private TopologyTestDriver topologyTestDriver;


    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(topology, config.asProperties());
        inputTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());
        bettorOutputTopic =
                topologyTestDriver.createOutputTopic(BETTOR_MONEY_TOPIC, Serdes.String().deserializer(),
                        new JsonSerde<>(Long.class).deserializer());
        commandOutputTopic =
                topologyTestDriver.createOutputTopic(COMMAND_MONEY_TOPIC, Serdes.String().deserializer(),
                        new JsonSerde<>(Long.class).deserializer());
        scoreTopic =
                topologyTestDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(),
                        new JsonSerde<>(EventScore.class).serializer());
        fraudTopic =
                topologyTestDriver.createOutputTopic(FRAUD_TOPIC, Serdes.String().deserializer(),
                        new JsonSerde<>(Fraud.class).deserializer());
    }

    @AfterEach
    public void closeTopologyTestDriver(){
        topologyTestDriver.close();
    }

    @Test
    void testSimpleBettor() {
        Bet bet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.7).build();

        inputTopic.pipeInput(bet.key(), bet);
        TestRecord<String, Long> record = bettorOutputTopic.readRecord();
        assertEquals("Kirill Grosul", record.key());
        assertEquals(100L, record.value().longValue());
    }

    @Test
    void testIterationsBettor() {
        final int ITERATIONS = 100;
        Bet bet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.7).build();

        for (int i = 1; i < ITERATIONS; ++i) {
            inputTopic.pipeInput(String.valueOf(i), bet);
            TestRecord<String, Long> record = bettorOutputTopic.readRecord();
            assertEquals("Kirill Grosul", record.key());
            assertEquals(100L * i, record.value().longValue());
        }
    }

    @Test
    void testSimpleBettorKeys() {
        Bet firstBet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.7).build();
        Bet secondBet = Bet.builder()
                .bettor("Ivan Ivanov")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(200)
                .odds(1.7).build();

        inputTopic.pipeInput("first", firstBet);
        inputTopic.pipeInput("second", secondBet);

        TestRecord<String, Long> record = bettorOutputTopic.readRecord();
        assertEquals("Kirill Grosul", record.key());
        assertEquals(100L, record.value().longValue());

        record = bettorOutputTopic.readRecord();
        assertEquals("Ivan Ivanov", record.key());
        assertEquals(200L, record.value().longValue());
    }

    @Test
    void testSimpleCommandHome() {
        Bet bet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.7).build();

        inputTopic.pipeInput(bet.key(), bet);
        TestRecord<String, Long> record = commandOutputTopic.readRecord();
        assertEquals("Germany", record.key());
        assertEquals(100L, record.value().longValue());
        assertThrows(NoSuchElementException.class, () -> {commandOutputTopic.readRecord();});
    }

    @Test
    void testSimpleCommandAway() {
        Bet bet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.A)
                .amount(200)
                .odds(1.7).build();

        inputTopic.pipeInput(bet.key(), bet);
        TestRecord<String, Long> record = commandOutputTopic.readRecord();
        assertEquals("Belgium", record.key());
        assertEquals(200L, record.value().longValue());
        assertThrows(NoSuchElementException.class, () -> {commandOutputTopic.readRecord();});
    }

    @Test
    void testSimpleCommandDraft() {
        Bet homeBet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(200)
                .odds(1.7).build();

        Bet draftBet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.D)
                .amount(200)
                .odds(1.7).build();


        inputTopic.pipeInput("first", homeBet);
        inputTopic.pipeInput("second", draftBet);

        TestRecord<String, Long> record = commandOutputTopic.readRecord();
        assertEquals("Germany", record.key());
        assertEquals(200L, record.value().longValue());
        assertThrows(NoSuchElementException.class, () -> {commandOutputTopic.readRecord();});
    }

    @Test
    void testIterationsCommand() {
        final int ITERATIONS = 100;

        Bet homeBet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(200)
                .odds(1.7).build();

        Bet draftBet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.D)
                .amount(200)
                .odds(1.7).build();

        Bet awayBet = Bet.builder()
                .bettor("Kirill Grosul")
                .match("Germany-Belgium")
                .outcome(Outcome.A)
                .amount(200)
                .odds(1.7).build();

        for (int i = 1; i < ITERATIONS; ++i) {
            inputTopic.pipeInput(homeBet);
            inputTopic.pipeInput(draftBet);
            inputTopic.pipeInput(awayBet);
            TestRecord<String, Long> record;

            record = commandOutputTopic.readRecord();
            assertEquals("Germany", record.key());
            assertEquals(200L * i, record.value().longValue());

            record = commandOutputTopic.readRecord();
            assertEquals("Belgium", record.key());
            assertEquals(200L * i, record.value().longValue());
            assertThrows(NoSuchElementException.class, () -> {commandOutputTopic.readRecord();});
        }

    }

    void putBet(Bet value) {
        inputTopic.pipeInput(value.key(), value);
    }

    void putScore(EventScore value) {
        scoreTopic.pipeInput(value.getEvent(), value);
    }


    @Test
    public void nearBetsFound() {
        long current = System.currentTimeMillis();
        putScore(new EventScore("Turkey-Moldova", new Score().goalHome(), current));
        putBet(new Bet("alice", "Turkey-Moldova", Outcome.A, 1, 1.5, current - 100));
        putBet(new Bet("bob", "Turkey-Moldova", Outcome.H, 1, 1.5, current - 100));
        putBet(new Bet("bob", "Turkey-Moldova", Outcome.H, 1, 1.5, current - 5000));
        Fraud exppectedFraud = Fraud.builder()
                .bettor("bob")
                .match("Turkey-Moldova")
                .outcome(Outcome.H)
                .amount(1)
                .odds(1.5)
                .lag(100)
                .build();


        assertEquals(exppectedFraud, fraudTopic.readValue());
        assertTrue(fraudTopic.isEmpty());
    }



    @Test
    void testFraud() {
        long currentTimestamp = System.currentTimeMillis();
        Score score = new Score().goalHome();
        putScore(new EventScore("A-B", score, currentTimestamp));
        score = score.goalHome();
        putScore(new EventScore("A-B", score, currentTimestamp + 100 * 1000));
        score = score.goalAway();
        putScore(new EventScore("A-B", score, currentTimestamp + 200 * 1000));
        //ok
        putBet(new Bet("John", "A-B", Outcome.H, 1, 1, currentTimestamp - 2000));
        //ok
        putBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 2000));
        //fraud?
        putBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 10));
        //fraud?
        putBet(new Bet("Mary", "A-B", Outcome.A, 1, 1, currentTimestamp + 200 * 1000 - 20));
        Fraud expected1 = Fraud.builder()
                .bettor("Sara").match("A-B").outcome(Outcome.H).amount(1).odds(1)
                .lag(10)
                .build();
        Fraud expected2 = Fraud.builder()
                .bettor("Mary").match("A-B").outcome(Outcome.A).amount(1).odds(1)
                .lag(20)
                .build();
        List<KeyValue<String, Fraud>> expected = new ArrayList<>();
        expected.add(KeyValue.pair("Sara", expected1));
        expected.add(KeyValue.pair("Mary", expected2));
        List<KeyValue<String, Fraud>> actual = fraudTopic.readKeyValuesToList();
        assertEquals(expected, actual);
    }

}

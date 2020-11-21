package ru.curs.homework.configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.homework.transformer.ScoreTransformer;


import java.time.Duration;

import static ru.curs.counting.model.TopicNames.*;

@Configuration
public class TopologyConfiguration {

    private final static String BETTOR_MONEY_STORE = "bettor_money_store";
    private final static String COMMAND_MONEY_STORE = "command_money_store";


    @Autowired
    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        /* Cтавки */
        KStream<String, Bet> input = streamsBuilder.
                stream(BET_TOPIC,
                        Consumed.with(
                                Serdes.String(), new JsonSerde<>(Bet.class))
                                .withTimestampExtractor((record, previousTimestamp) ->
                                        ((Bet) record.value()).getTimestamp()
                                )
                );
        /*Key: "Germany-Belgium:H"
         Value: Bet{
                   bettor    = John Doe;
                   match     = Germany-Belgium;
                   outcome   = H;
                   amount    = 100;
                   odds      = 1.7;
                   timestamp = 1554215083873;
                }
        */

        /* Голы */
        KStream<String, EventScore> eventScores = streamsBuilder.stream(EVENT_SCORE_TOPIC,
                Consumed.with(
                        Serdes.String(), new JsonSerde<>(EventScore.class))
                        .withTimestampExtractor((record, previousTimestamp) ->
                                ((EventScore) record.value()).getTimestamp()));
        /*
            Key: Germany-Belgium
            Value: EventScore {
                     event     = Germany-Belgium;
                     score     = 1:1; <<<was 0:1
                     timestamp = 554215083998;
             }
         */



        /* Поток, превращающий ставку в пару (пользователь, сумма ставки) */
        KStream<String, Long> bettorGain =
                input.map((key, value) -> {
                    String name = value.getBettor();
                    Long amount = value.getAmount();
                    return KeyValue.pair(name, amount);
                });

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /* Таблица, по ключу выдающая сумму ставок, сделланных данным пользователем, 1-ая часть дз */
        KTable<String, Long> bettorAmount = bettorGain
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .reduce(Long::sum);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /* Поток, превращающий ставку в пару (команда, сумма ставки) */
        KStream<String, Long> commandGain = input
                .filter((key, value) -> value.getOutcome() != Outcome.D)
                .map((key, value) -> {
                    String[] commands = value.getMatch().split("-");
                    String gainCommand = (value.getOutcome() == Outcome.H) ? commands[0] : commands[1];
                    return KeyValue.pair(gainCommand, value.getAmount());
                });

        /* Таблица, по ключу выдающая сумму ставок, сделланных на данную команду, 2-ая часть дз */
        KTable<String, Long> commandAmount = commandGain
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .reduce(Long::sum);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        /* Поток ставок на выигрыш */
        KStream<String, Bet> winningBets = new ScoreTransformer().transformStream(streamsBuilder, eventScores);

        /* Поток подозрительных ставок, 3 часть дз */
        KStream<String, Fraud> fraud = input.
                join(winningBets, (bet, winningBet) ->
                                Fraud.builder()
                                        .bettor(bet.getBettor())
                                        .outcome(bet.getOutcome())
                                        .amount(bet.getAmount())
                                        .match(bet.getMatch())
                                        .odds(bet.getOdds())
                                        .lag(winningBet.getTimestamp() - bet.getTimestamp())
                                        .build(),
                        JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                        StreamJoined.with(Serdes.String(),
                                new JsonSerde<>(Bet.class),
                                new JsonSerde<>(Bet.class)
                        ))
                .selectKey((key, value) -> value.getBettor());
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        bettorAmount.toStream().to(BETTOR_MONEY_TOPIC, Produced.with(Serdes.String(),
                new JsonSerde<>(Long.class)));

        commandAmount.toStream().to(COMMAND_MONEY_TOPIC, Produced.with(Serdes.String(),
                new JsonSerde<>(Long.class)));

        fraud.to(TopicNames.FRAUD_TOPIC, Produced.with(Serdes.String(),
                new JsonSerde<>(Fraud.class)));


        Topology topology = streamsBuilder.build();
        System.out.println("========================================");
        System.out.println(topology.describe());
        System.out.println("========================================");
        return topology;
    }
}

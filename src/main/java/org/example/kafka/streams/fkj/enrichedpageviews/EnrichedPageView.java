package org.example.kafka.streams.fkj.enrichedpageviews;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EnrichedPageView {

    private int id;
    private String title;
    private String time;

}

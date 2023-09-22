package africa.absa.absaoss.kafkarest.controller;

import africa.absa.absaoss.kafkarest.model.DuckHuntEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Level;
import java.util.logging.Logger;

@RestController
class DuckHuntEventController {
    Logger logger = Logger.getLogger(DuckHuntEvent.class.getName());
    @PostMapping("/rest/postEvent")
    Boolean postEvent(@RequestBody DuckHuntEvent duckHuntEvent) {
        logger.log(Level.INFO, duckHuntEvent.toString());
        return true;
    }
}

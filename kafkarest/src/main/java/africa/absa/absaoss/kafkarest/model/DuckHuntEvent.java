package africa.absa.absaoss.kafkarest.model;

import java.util.Objects;

public class DuckHuntEvent {
    public enum EventType {
        SHOT,
        HIT
    }
    private String email;
    private EventType eventType;
    private Integer eventSize;

    public DuckHuntEvent() {
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Integer getEventSize() {
        return eventSize;
    }

    public void setEventSize(Integer eventSize) {
        this.eventSize = eventSize;
    }

    public DuckHuntEvent(String email, EventType eventType, Integer eventSize) {
        this.email = email;
        this.eventType = eventType;
        this.eventSize = eventSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DuckHuntEvent that = (DuckHuntEvent) o;
        return Objects.equals(email, that.email) && eventType == that.eventType && Objects.equals(eventSize, that.eventSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(email, eventType, eventSize);
    }

    @Override
    public String toString() {
        return "DuckHuntEvent{" +
                "email='" + email + '\'' +
                ", eventType=" + eventType +
                ", eventSize=" + eventSize +
                '}';
    }
}

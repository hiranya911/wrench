package edu.ucsb.cs.wrench.elections;

import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.messaging.Event;

public class VictoryEvent extends Event {

    private Member member;

    public VictoryEvent(String member) {
        this.member = WrenchConfiguration.getConfiguration().getMember(member);
    }

    public Member getMember() {
        return member;
    }
}

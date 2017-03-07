package com.pilosa.client;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

public final class ProfileItem {
    private long id;
    private Map<String, Object> attributes;

    ProfileItem() {
    }

    ProfileItem(long id, Map<String, Object> attributes) {
        this.id = id;
        this.attributes = attributes;
    }

    static ProfileItem fromInternal(Internal.Profile profile) {
        return new ProfileItem(profile.getID(), Util.protobufAttrsToMap(profile.getAttrsList()));
    }

    /**
     * Returns profile ID.
     *
     * @return profile ID
     */
    public long getID() {
        return this.id;
    }

    /**
     * Returns profile attributes.
     *
     * @return profile attributes
     */
    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    @Override
    public String toString() {
        return String.format("ProfileItem(id=%d, attrs=%s)", this.id, this.attributes);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.id)
                .append(this.attributes)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ProfileItem)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        ProfileItem rhs = (ProfileItem) obj;
        return new EqualsBuilder()
                .append(this.id, rhs.id)
                .append(this.attributes, rhs.attributes)
                .isEquals();
    }
}

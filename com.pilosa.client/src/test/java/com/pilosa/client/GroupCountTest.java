/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class GroupCountTest {
    @Test
    public void testGroupCount() {
        List<FieldRow> groups = Collections.singletonList(FieldRow.create("f1", 42));
        GroupCount g = GroupCount.create(groups, 10);
        assertEquals(Collections.singletonList(FieldRow.create("f1", 42)),
                g.getGroups());
        assertEquals(10, g.getCount());
        assertEquals("GroupCount(groups=[FieldRow(field=f1, rowID=42, rowKey=)], count=10)",
                g.toString());
    }

    @Test
    public void testEquals() {
        List<FieldRow> groups = Collections.singletonList(FieldRow.create("f1", 42));
        GroupCount g = GroupCount.create(groups, 10);
        assertTrue(g.equals(g));
        assertFalse(g.equals(new Integer(10)));

    }

    @Test
    public void testHashCode() {
        List<FieldRow> groups = Collections.singletonList(FieldRow.create("f1", 42));
        GroupCount g1 = GroupCount.create(groups, 10);
        GroupCount g2 = GroupCount.create(groups, 10);
        assertEquals(g1.hashCode(), g2.hashCode());

    }
}

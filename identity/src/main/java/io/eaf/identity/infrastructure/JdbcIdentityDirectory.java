package io.eaf.identity.infrastructure;

import io.eaf.identity.api.IdentityDirectory;
import io.eaf.organization.api.OrganizationDirectory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcIdentityDirectory implements IdentityDirectory {
    private final JdbcTemplate jdbc;
    private final OrganizationDirectory organizations;

    public JdbcIdentityDirectory(JdbcTemplate jdbc, OrganizationDirectory organizations) {
        this.jdbc = jdbc;
        this.organizations = organizations;
    }

    @Override
    public Map<UUID, String> displayNames(UUID tenantId, Set<UUID> subjectIds) {
        if (tenantId == null || subjectIds == null || subjectIds.isEmpty() || subjectIds.size() > 100)
            return Map.of();
        var result = new LinkedHashMap<UUID, String>();
        for (var subjectId : subjectIds) {
            // 身份域只查自己的资料；当前租户成员有效性由 Organization 公共 API 提供。
            if (!organizations.isActiveMember(tenantId, subjectId)) continue;
            jdbc.query("select display_name from \"identity\".subject "
                            + "where id = ? and type = 'HUMAN' and status = 'ACTIVE'",
                    rs -> { if (rs.next()) result.put(subjectId, rs.getString(1)); }, subjectId);
        }
        return Map.copyOf(result);
    }

    @Override
    public boolean isActiveHuman(UUID tenantId, UUID subjectId) {
        if (tenantId == null || subjectId == null || !organizations.isActiveMember(tenantId, subjectId)) return false;
        var count = jdbc.queryForObject("select count(*) from \"identity\".subject where id = ? and type = 'HUMAN' and status = 'ACTIVE'",
                Integer.class, subjectId);
        return count != null && count == 1;
    }
}

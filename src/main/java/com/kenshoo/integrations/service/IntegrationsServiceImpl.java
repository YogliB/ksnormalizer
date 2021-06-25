package com.kenshoo.integrations.service;

import com.kenshoo.integrations.dao.IntegrationsDao;
import com.kenshoo.integrations.entity.Integration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IntegrationsServiceImpl implements IntegrationsService{
    private IntegrationsDao _dao = new IntegrationsDao();
    private KsNormalizerClient _ksNormalizer = new KsNormalizerClient();

    /**
     * Inserts data into the integrations table
     *
     * @param ksId a ks id, might be not normalized
     * @param data data to be inserted
     */
    public void insertIntegration(String ksId, String data) {
        try {
            String normalizedKsId = this._ksNormalizer.normalize(ksId);
            this._dao.insert(normalizedKsId, data);
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

    }

    /**
     * Returns all integrations having the provided ks id
     *
     * @param ksId a ks id, might be not normalized
     * @return list of all the integrations having the provided ks id
     */
    public List<Integration> fetchIntegrationsByKsId(String ksId) {
        List<Integration> integrations = new ArrayList<>();

        try {
            String normalizedKsId = this._ksNormalizer.normalize(ksId);
            integrations = this._dao.fetchByKsId(normalizedKsId);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        return integrations;
    }

    /**
     * Updates all rows in integrations table with normalized ks id
     *
     * @return number of affected rows
     */
    public int migrate() {
        List<Integration> integrations = this._dao.fetchAll();
        int count = 0;

        for(Integration integration: integrations) {
            try {
                String ksId = integration.getKsId();
                String normalizedKsId = this._ksNormalizer.normalize(ksId);
                boolean shouldMigrate = !ksId.trim().equals(normalizedKsId.trim());

                if (shouldMigrate) {
                    this._dao.updateKsId(ksId, normalizedKsId);
                    count ++;
                }
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return  count;
    }
}
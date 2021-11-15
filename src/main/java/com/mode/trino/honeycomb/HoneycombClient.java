package com.mode.trino.honeycomb;

import java.util.List;
import java.util.Optional;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface HoneycombClient {
    String DEFAULT_HONEYCOMB_BASE_URL = "https://api.honeycomb.io/";
    String HONEYCOMB_KEY_HEADER = "X-Honeycomb-Team";

    @GET("/1/auth")
    Call<HoneycombKey> getKeyDetails();

    @GET("/1/datasets")
    Call<List<HoneycombDataset>> getAllDatasets();

    @GET("/1/columns/{dataset}")
    Call<List<HoneycombAttribute>> getDatasetAttributes(@Path("dataset") String dataset);

    static HoneycombClient retrofit(String apiKey, Optional<String> apiUrl) {
        OkHttpClient client = new OkHttpClient.Builder()
            .addInterceptor(chain -> {
                Request request = chain.request().newBuilder()
                    .header(HONEYCOMB_KEY_HEADER, apiKey)
                    .build();
                return chain.proceed(request);
            })
            .build();

        Retrofit retrofit = new Retrofit.Builder()
            .addConverterFactory(JacksonConverterFactory.create())
            .baseUrl(apiUrl.orElse(DEFAULT_HONEYCOMB_BASE_URL))
            .client(client)
            .build();

        return retrofit.create(HoneycombClient.class);
    }
}

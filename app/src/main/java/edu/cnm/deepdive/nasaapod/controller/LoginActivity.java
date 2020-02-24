package edu.cnm.deepdive.nasaapod.controller;

import androidx.appcompat.app.AppCompatActivity;
import android.os.Bundle;
import edu.cnm.deepdive.nasaapod.R;
import edu.cnm.deepdive.nasaapod.service.GoogleSignInRepository;

public class LoginActivity extends AppCompatActivity {

  private static final int LOGIN_REQUEST_CODE = 3262;

  private GoogleSignInRepository repository;

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    repository = GoogleSignInRepository.getInstance();
    repository.refresh()
        .addOnSuccessListener((account) -> { /* TODO Switch to main activity */ })
        .addOnFailureListener((ex) -> {
          setContentView(R.layout.activity_login);
          findViewById(R.id.sign_in).setOnClickListener((v) ->
              repository.startSignIn(this, LOGIN_REQUEST_CODE));
        });
  }
}

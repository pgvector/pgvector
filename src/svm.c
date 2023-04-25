/* This code demonstrates a simple SVM implementation in C, which you can use as a starting point for further development. The code is divided into several functions:

   1. `kernel_function`: calculates the dot product of two data points.
   2. `smo`: trains the SVM model using the Sequential Minimal Optimization algorithm.
   3. `svm_predict`: makes predictions using the trained SVM model.
   4. `main`: demonstrates how to use the above functions to train an SVM model and make predictions.

   You can modify the `data` array and other parameters in the `main` function to test the SVM model on different datasets. Note that this is a simple implementation and might not be suitable for larger datasets or more complex problems. */

#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define N 100
#define MAX_ITER 1000
#define TOL 1e-6

typedef struct {
    double x[N];
    double y;
} data_point;

double kernel_function(const data_point *a, const data_point *b, int dim) {
    double result = 0;
    for (int i = 0; i < dim; i++) {
        result += a->x[i] * b->x[i];
    }
    return result;
}

void smo(data_point *data, double *alpha, double *b, int size, int dim, double C) {
    int num_changed_alphas = 0;
    int examine_all = 1;
    int iter = 0;

    while (num_changed_alphas > 0 || examine_all) {
        num_changed_alphas = 0;
        if (examine_all) {
            for (int i = 0; i < size; i++) {
                double Ei = 0;
                for (int j = 0; j < size; j++) {
                    Ei += alpha[j] * data[j].y * kernel_function(&data[i], &data[j], dim);
                }
                Ei -= *b + data[i].y;
                double ai_old = alpha[i];
                alpha[i] += data[i].y * Ei / kernel_function(&data[i], &data[i], dim);
                alpha[i] = fmax(0, fmin(C, alpha[i]));

                if (fabs(alpha[i] - ai_old) > TOL) {
                    num_changed_alphas++;
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                if (alpha[i] != 0 && alpha[i] != C) {
                    double Ei = 0;
                    for (int j = 0; j < size; j++) {
                        Ei += alpha[j] * data[j].y * kernel_function(&data[i], &data[j], dim);
                    }
                    Ei -= *b + data[i].y;
                    double ai_old = alpha[i];
                    alpha[i] += data[i].y * Ei / kernel_function(&data[i], &data[i], dim);
                    alpha[i] = fmax(0, fmin(C, alpha[i]));

                    if (fabs(alpha[i] - ai_old) > TOL) {
                        num_changed_alphas++;
                    }
                }
            }
        }

        if (examine_all == 1) {
            examine_all = 0;
        } else if (num_changed_alphas == 0) {
            examine_all = 1;
        }

        iter++;
        if (iter >= MAX_ITER) {
            break;
        }
    }

    // Compute b
    double sum = 0;
    int count = 0;
    for (int i = 0; i < size; i++) {
        if (alpha[i] > 0 && alpha[i] < C) {
            double temp = 0;
            for (int j = 0; j < size; j++) {
                temp += alpha[j] * data[j].y * kernel_function(&data[i], &data[j], dim);
            }
            sum += data[i].y
              temp;
            count++;
        }
    }
    *b = sum / count;
}

double svm_predict(data_point *data, data_point *test_point, double *alpha, double b, int size, int dim) {
  double result = 0;
  for (int i = 0; i < size; i++) {
    result += alpha[i] * data[i].y * kernel_function(&data[i], test_point, dim);
  }
  result -= b;
  return result > 0 ? 1 : -1;
}

/*int main() {
  int size = 4;
  int dim = 2;
  double C = 1;

  data_point data[] = {
    {{0, 0}, 1},
    {{1, 1}, 1},
    {{2, 2}, -1},
    {{3, 3}, -1}
  };

  double alpha[N] = {0};
  double b = 0;

  smo(data, alpha, &b, size, dim, C);

  data_point test_point = {{1.5, 1.5}, 0};
  double prediction = svm_predict(data, &test_point, alpha, b, size, dim);

  printf("Predicted class: %lf\n", prediction);

  return 0;
}*/



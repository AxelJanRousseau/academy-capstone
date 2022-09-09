locals {
  project_name = "axeljan_project"
  uuid_pattern = "????????-????-????-????-????????????"
}
resource "aws_iam_role" "default" {
  name               = "${local.project_name}-${var.env_name}"
  assume_role_policy = data.aws_iam_policy_document.default_assume_role.json
}
data "aws_iam_policy_document" "default_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"
    condition {
      test     = "StringLike"
      variable = "${replace(var.aws_iam_openid_connect_provider_url, "https://", "")}:sub"
      values   = [
        "system:serviceaccount:${var.env_name}:${replace(local.project_name, "_", ".")}-${local.uuid_pattern}"
      ]
    }
    principals {
      identifiers = [var.aws_iam_openid_connect_provider_arn]
      type        = "Federated"
    }
  }
}
resource "aws_iam_role_policy" "default" {
  name   = "${local.project_name}-${var.env_name}"
  role   = aws_iam_role.default.id
  policy = data.aws_iam_policy_document.default.json
}
data "aws_iam_policy_document" "default" {
  statement {
    actions = [
      "s3:*Get*"
    ]
    resources = [
      "arn:aws:s3:::dataminded-academy-capstone-resources2/raw/*",
    ]
    effect = "Allow"
  }
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetEncryptionConfiguration",
      "s3:GetBucketLocation"
    ]
    resources = ["arn:aws:s3:::dataminded-academy-capstone-resources2"]
    effect    = "Allow"
  }
  statement {
    effect    = "Allow"
    actions   = ["s3:ListAllMyBuckets"]
    resources = ["*"]
  }
  statement {
    effect    = "Allow"
    actions   = ["s3:GetAccountPublicAccessBlock"]
    resources = ["*"]
  }
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:ListSecrets"]
    resources = ["*"]
  }
  statement {
    effect  = "Allow"
    actions = [
      "secretsmanager:ListSecretVersionIds",
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = ["arn:aws:secretsmanager:*:*:secret:snowflake/capstone/login*"]
  }
  statement {
    effect = "Allow"
    actions = ["kms:*Decrypt*"]
    resources = ["*"]
  }
}